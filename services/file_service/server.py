import grpc
from concurrent import futures
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
import os, json, threading, uuid
import math
import time, random

from .utils import DATA_DIR, SUBDIVISIONS_DIR, META_PATH, file_timestamp, load_meta, save_meta
from .raft import RaftManager

SERVER_ID = os.environ.get(key="SERVER_ID", default="server-1")
PEERS = [p for p in os.environ.get("PEERS", "").split(",") if p]
RPC_PORT = os.environ.get("RPC_PORT", "50051")
# Address other containers can reach this server on; handed to clients as the leader hint
ADVERTISE_ADDR = os.environ.get("ADVERTISE_ADDR", f"localhost:{RPC_PORT}")

# Reconnect quickly once a peer comes back instead of gRPC's default backoff of up to 2 minutes,
# and re-resolve DNS promptly since a restarted container may come back on a new IP.
PEER_CHANNEL_OPTIONS = [
    ('grpc.max_send_message_length', -1),
    ('grpc.max_receive_message_length', -1),
    ('grpc.initial_reconnect_backoff_ms', 500),
    ('grpc.min_reconnect_backoff_ms', 500),
    ('grpc.max_reconnect_backoff_ms', 2000),
    ('grpc.dns_min_time_between_resolutions_ms', 500),
]

class FileServiceServicer(file_service_pb2_grpc.FileServiceServicer):
    def __init__(self):
        self.meta = load_meta()

        if "file_versions" not in self.meta:
            self.meta["file_versions"] = {}

        self.lock = threading.RLock()

        self.server_id = SERVER_ID
        self.advertise_addr = ADVERTISE_ADDR
        self.cluster_size = len(PEERS) + 1
        self.peers = PEERS[:]
        # One long-lived channel per peer, shared by raft and replication
        self.peer_stubs = {
            p: file_service_pb2_grpc.FileServiceStub(grpc.insecure_channel(p, options=PEER_CHANNEL_OPTIONS))
            for p in self.peers
        }

        self.current_term = self.meta.get("term", 0)
        self.voted_for = self.meta.get("voted_for", None)

        # Every committed upload gets the next index in one cluster-wide sequence, tagged with
        # the term it was committed in; (commit_term, commit_index) orders how up to date we are
        self.commit_index = self.meta.get("commit_index", 0)
        self.commit_term = self.meta.get("commit_term", 0)
        # An upload this follower has stored but not yet been told is committed
        self.pending = self.meta.get("pending", None)
        # Followers refuse reads until a heartbeat confirms they hold every committed upload
        self.caught_up = False
        # Serialises uploads on the leader so each one gets the next index
        self.upload_lock = threading.Lock()

        # Every node starts as a follower and the cluster elects a leader. The last_leader
        # saved in meta is not trusted: a stale hint just sends clients to a dead server.
        self.role = "follower"
        self.leader_host = None

        self._cleanup_temp_files()

        # Move election and heartbeat behavior into a dedicated manager
        self.raft = RaftManager(self)

    def persist(self):
        """Save raft state to meta.json. Caller should hold self.lock."""
        self.meta["term"] = self.current_term
        self.meta["voted_for"] = self.voted_for
        self.meta["last_leader"] = self.leader_host
        self.meta["commit_index"] = self.commit_index
        self.meta["commit_term"] = self.commit_term
        self.meta["pending"] = self.pending
        save_meta(self.meta)

    def last_entry(self):
        """(term, index) of the newest upload this server holds, committed or not. Caller should hold self.lock."""
        if self.pending:
            return (self.pending["term"], self.pending["index"])
        return (self.commit_term, self.commit_index)

    def _pending_tmp_name(self):
        if not self.pending:
            return None
        return f"{self.pending['filename']}.{self.pending['request_id']}.tmp"

    def apply_pending(self):
        """Commit the pending upload: move it into place and record its version. Caller should hold self.lock."""
        p = self.pending
        final_path = os.path.join(DATA_DIR, p["filename"])
        os.replace(os.path.join(DATA_DIR, self._pending_tmp_name()), final_path)
        self.meta["file_versions"][p["filename"]] = p["version"]
        self.meta.setdefault("file_sizes", {})[p["filename"]] = os.path.getsize(final_path)
        self.commit_index, self.commit_term = p["index"], p["term"]
        self.pending = None
        self.persist()

    def _discard_pending(self):
        """Drop an upload that will never be committed. Caller should hold self.lock and persist."""
        tmp_path = os.path.join(DATA_DIR, self._pending_tmp_name())
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        self.pending = None

    def _accept_leader(self, term):
        """Called for every RPC sent by a leader. Returns False if that leader is stale.
        Caller should hold self.lock."""
        if term < self.current_term:
            return False
        # A valid leader exists for this term, so any candidate (or stale leader) steps down
        if term > self.current_term or self.role != "follower":
            self.step_down(term)
        self.raft.heard_from_leader()
        return True

    def _learn_leader_commit(self, commit_index, commit_term):
        """Apply our pending upload if the leader has committed it, then work out whether we are
        missing any committed uploads. Caller should hold self.lock."""
        if self.pending and (self.pending["term"], self.pending["index"]) == (commit_term, commit_index):
            self.apply_pending()
        caught_up = (self.commit_term, self.commit_index) == (commit_term, commit_index)
        if caught_up != self.caught_up:
            if caught_up:
                print(f"[Follower] {self.server_id} is up to date (index {self.commit_index})")
            else:
                print(f"[Follower] {self.server_id} is behind the leader (index {self.commit_index}, "
                      f"leader at {commit_index}); refusing reads until synced")
        self.caught_up = caught_up

    def _require_caught_up(self, context):
        """Refuse reads on a follower that may be missing committed uploads. Clients fail over
        to another server on UNAVAILABLE."""
        with self.lock:
            serving = self.role == "leader" or self.caught_up
        if not serving:
            context.abort(grpc.StatusCode.UNAVAILABLE, "replica is catching up with the leader")

    def step_down(self, term):
        """Become a follower, adopting term if it is newer. Caller should hold self.lock."""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.leader_host = None
            self.persist()
        self.role = "follower"

    def _call_peers(self, method, request, timeout):
        """Send the same RPC to every peer in parallel. Returns {peer: response, or None if it failed}."""
        calls = {peer: getattr(stub, method).future(request, timeout=timeout)
                 for peer, stub in self.peer_stubs.items()}
        results = {}
        for peer, fut in calls.items():
            try:
                results[peer] = fut.result()
            except grpc.RpcError as e:
                print(f"[{self.server_id}] {method} to {peer} failed: {e.code().name}")
                results[peer] = None
        return results

    def _notify_peers(self, method, request, timeout):
        """Best-effort: send the same RPC to every peer in parallel without waiting for the replies."""
        def log_failure(fut, peer):
            if fut.exception() is not None:
                print(f"[{self.server_id}] {method} to {peer} failed: {fut.code().name}")

        for peer, stub in self.peer_stubs.items():
            fut = getattr(stub, method).future(request, timeout=timeout)
            fut.add_done_callback(lambda f, p=peer: log_failure(f, p))


    def _cleanup_temp_files(self):
        # Keep the pending upload's data: it may already be committed on the rest of the cluster
        keep = self._pending_tmp_name()
        for filename in os.listdir(DATA_DIR):
            if filename.endswith('.tmp') and filename != keep:
                tmp_path = os.path.join(DATA_DIR, filename)
                os.remove(tmp_path)
                print(f"[startup] Cleaned up temp file: {filename}")
        if keep and not os.path.exists(os.path.join(DATA_DIR, keep)):
            print(f"[startup] Data for pending upload {keep} is missing, dropping it")
            self.pending = None
            self.persist()

    def Heartbeat(self, request, context):
        with self.lock:
            # Reject heartbeats from a stale leader; the reply tells it the newer term
            if not self._accept_leader(request.term):
                return file_service_pb2.HeartbeatResponse(ok=False, term=self.current_term)

            # Only touch the disk when something actually changed
            if self.leader_host != request.leader_id:
                self.leader_host = request.leader_id
                self.persist()
                print(f"[Follower] {self.server_id} following leader {self.leader_host} (term {self.current_term})")

            self._learn_leader_commit(request.leader_commit_index, request.leader_commit_term)
            return file_service_pb2.HeartbeatResponse(
                ok=True, term=self.current_term, commit_index=self.commit_index, commit_term=self.commit_term)

    def RequestVote(self, request, context):
        with self.lock:
            # Only vote for a candidate holding every upload we hold, so a server that missed
            # committed uploads can never be elected
            up_to_date = (request.last_term, request.last_index) >= self.last_entry()

            if request.pre_vote:
                # Answer without changing any state. Refuse while a leader is still heartbeating
                # us, so a node that only lost contact itself can't start an election.
                granted = (request.term > self.current_term and up_to_date
                           and not self.raft.leader_recently_seen())
                return file_service_pb2.VoteResponse(vote_granted=granted, term=self.current_term)

            # If the request's term is older, deny immediately
            if request.term < self.current_term:
                return file_service_pb2.VoteResponse(vote_granted=False, term=self.current_term)

            # A higher term means our term (and any leadership in it) is over
            if request.term > self.current_term:
                self.step_down(request.term)

            # Grant vote if we haven't voted yet in this term or we already voted for this candidate
            if not up_to_date:
                print(f"[Election] Refused {request.candidate_id} in term {self.current_term}: it is missing uploads we have")
            elif self.voted_for in (None, request.candidate_id):
                self.voted_for = request.candidate_id
                self.persist()
                # Don't start a rival election while this candidate is still counting votes
                self.raft.reset_election_timer()
                print(f"[Election] Voted for {request.candidate_id} in term {self.current_term}")
                return file_service_pb2.VoteResponse(vote_granted=True, term=self.current_term)

            # Otherwise deny vote
            return file_service_pb2.VoteResponse(vote_granted=False, term=self.current_term)



    def CreateFile(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        if os.path.exists(path):
            return file_service_pb2.CreateResponse(success=False, message="File already exists")
        with open(path, "wb") as f:
            f.write(request.data)
        return file_service_pb2.CreateResponse(success=True, message="File created")

    def OpenFile(self, request, context):
        self._require_caught_up(context)
        path = os.path.join(DATA_DIR, request.filename)
        if not os.path.exists(path):
            return file_service_pb2.OpenResponse(success=False, message="No such file")
        with open(path, "rb") as f:
            data = f.read()
        ts = file_timestamp(path)
        ver = self.meta.get("file_versions", {}).get(request.filename, 0)
        return file_service_pb2.OpenResponse(success=True, data=data, message="File sent", server_timestamp=ts, server_version=ver)

    def UploadFile(self, request, context):
        filename = request.filename
        data = request.data

        with self.lock:
            if self.role != "leader":
                hint = self.leader_host or ""
                print(f"Redirecting UploadFile to leader {hint}")
                return file_service_pb2.UploadResponse(
                    success=False,
                    message="NOT_LEADER",
                    leader_host = hint
                )
        
        # One upload at a time, so each gets the next index in the commit sequence
        with self.upload_lock:
            return self._replicate_upload(filename, data)

    def _replicate_upload(self, filename, data):
        with self.lock:
            # leadership may have changed while we waited for the upload lock
            if self.role != "leader":
                return file_service_pb2.UploadResponse(success=False, message="NOT_LEADER", leader_host=self.leader_host or "")
            term = self.current_term
            index = self.commit_index + 1
            version = self.meta["file_versions"].get(filename, 0) + 1
            commit_index, commit_term = self.commit_index, self.commit_term
        print(f"[Leader] handling UploadFile for {filename} (index {index}, version {version})")

        request_id = str(int(time.time() * 1000)) + "-" + uuid.uuid4().hex[:8]
        tmp_name = f"{filename}.{request_id}.tmp"
        tmp_path = os.path.join(DATA_DIR, tmp_name)

        try:
            with open(tmp_path, "wb") as f:
                f.write(data)
                f.flush(); os.fsync(f.fileno())
        except Exception as e:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

            return file_service_pb2.UploadResponse(success=False, message=f"Local write failed: {e}")

        # A dead follower only costs its ack; the upload succeeds as long as a majority stores it
        replicated = self._call_peers(
            "ReplicateFile",
            file_service_pb2.ReplicateRequest(
                filename=filename, data=data, request_id=request_id, term=term, index=index, version=version,
                leader_commit_index=commit_index, leader_commit_term=commit_term),
            timeout=5)
        acks = 1 + sum(1 for resp in replicated.values() if resp is not None and resp.success)
        cleanup_req = file_service_pb2.CleanupTempRequest(filename=filename, request_id=request_id)

        majority = (self.cluster_size // 2) + 1
        if acks < majority:
            os.remove(tmp_path)
            self._notify_peers("CleanupTemp", cleanup_req, timeout=1)
            return file_service_pb2.UploadResponse(
                success=False,
                message=f"Failed to replicate {filename} to majority"
            )

        final_path = os.path.join(DATA_DIR, filename)
        with self.lock:
            # A newer leader may have taken over while we replicated; it decides this upload's fate
            if self.role != "leader" or self.current_term != term:
                os.remove(tmp_path)
                return file_service_pb2.UploadResponse(success=False, message="NOT_LEADER", leader_host=self.leader_host or "")
            try:
                os.replace(tmp_path, final_path)
                self.meta.setdefault("file_sizes", {})[filename] = os.path.getsize(final_path)
                self.meta["file_versions"][filename] = version
                self.commit_index, self.commit_term = index, term
                self.persist()
            except Exception as e:
                self._notify_peers("CleanupTemp", cleanup_req, timeout=1)
                return file_service_pb2.UploadResponse(success=False, message=f"Commit failed locally: {e}")

        # Best-effort: a follower that misses this applies the upload from the next heartbeat instead
        self._notify_peers(
            "CommitFile",
            file_service_pb2.CommitRequest(filename=filename, request_id=request_id, version=version, index=index),
            timeout=2)

        return file_service_pb2.UploadResponse(success=True, message="Uploaded and replicated (committed)")

    def ReplicateFile(self, request, context):
    
        filename = request.filename
        request_id = request.request_id
        data = request.data
        tmp_name = f"{filename}.{request_id}.tmp"
        tmp_path = os.path.join(DATA_DIR, tmp_name)

        with self.lock:
            if not self._accept_leader(request.term):
                return file_service_pb2.UploadResponse(success=False, message="STALE_LEADER")
            self._learn_leader_commit(request.leader_commit_index, request.leader_commit_term)
            if not self.caught_up:
                # Missing earlier uploads: the leader syncs us after its next heartbeat
                return file_service_pb2.UploadResponse(success=False, message="BEHIND")
            # Anything still pending here was never committed; the leader has moved past it
            if self.pending:
                self._discard_pending()
                self.persist()

        try:
            with open(tmp_path, "wb") as f:
                f.write(data)
                f.flush(); os.fsync(f.fileno())
        except Exception as e:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            print(f"[Follower] Replication error: {e}")
            return file_service_pb2.UploadResponse(success=False, message=str(e))

        with self.lock:
            # a sync or a new leader may have moved our state on while we were writing
            if not self.caught_up or self.current_term != request.term:
                os.remove(tmp_path)
                return file_service_pb2.UploadResponse(success=False, message="BEHIND")
            self.pending = {"index": request.index, "term": request.term, "filename": filename,
                            "version": request.version, "request_id": request_id}
            self.persist()
        return file_service_pb2.UploadResponse(success=True, message="tmp stored")

    def CommitFile(self, request, context):
        
        request_id = request.request_id

        with self.lock:
            p = self.pending
            if not p or p["index"] != request.index or p["request_id"] != request_id:
                return file_service_pb2.CommitResponse(success=False, message="no matching pending upload")
            try:
                self.apply_pending()
            except Exception as e:
                return file_service_pb2.CommitResponse(success=False, message=str(e))
        return file_service_pb2.CommitResponse(success=True, message="committed")

    def CleanupTemp(self, request, context):
        request_id = getattr(request, "request_id", None)
        filename = request.filename
        with self.lock:
            if self.pending and self.pending["request_id"] == request_id:
                self._discard_pending()
                self.persist()
        
        if request_id:
            tmp_name = f"{filename}.{request_id}.tmp"
            tmp_path = os.path.join(DATA_DIR, tmp_name)
            
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        return file_service_pb2.CleanupTempResponse(ok=True, message="cleaned")

    def GetFileVersions(self, request, context):
        with self.lock:
            return file_service_pb2.FileVersionsResponse(
                versions=dict(self.meta["file_versions"]),
                commit_index=self.commit_index, commit_term=self.commit_term)

    def InstallFile(self, request, context):
        """Sync from the leader: overwrite one file with the leader's copy and version."""
        with self.lock:
            if not self._accept_leader(request.term):
                return file_service_pb2.InstallFileResponse(success=False, term=self.current_term)
        final_path = os.path.join(DATA_DIR, request.filename)
        tmp_path = f"{final_path}.sync.tmp"
        with open(tmp_path, "wb") as f:
            f.write(request.data)
            f.flush(); os.fsync(f.fileno())
        with self.lock:
            os.replace(tmp_path, final_path)
            self.meta["file_versions"][request.filename] = request.version
            self.meta.setdefault("file_sizes", {})[request.filename] = len(request.data)
            save_meta(self.meta)
            return file_service_pb2.InstallFileResponse(success=True, term=self.current_term)

    def FinishSync(self, request, context):
        """Sync from the leader is complete: adopt its commit point."""
        with self.lock:
            if not self._accept_leader(request.term):
                return file_service_pb2.FinishSyncResponse(success=False, term=self.current_term)
            if self.pending:
                self._discard_pending()
            self.commit_index, self.commit_term = request.commit_index, request.commit_term
            self.persist()
            self.caught_up = True
            print(f"[Follower] {self.server_id} synced with the leader up to index {self.commit_index}")
            return file_service_pb2.FinishSyncResponse(success=True, term=self.current_term)

    def TestAuth(self, request, context):
        self._require_caught_up(context)
        path = os.path.join(DATA_DIR, request.filename)
        if not os.path.exists(path):
            return file_service_pb2.TestAuthResponse(valid=False, message="File not found", server_version=0)
        # Only use logical version for TestAuth now
        server_ver = self.meta.get("file_versions", {}).get(request.filename, 0)
        client_ver = getattr(request, "client_version", None)
        if client_ver is None:
            return file_service_pb2.TestAuthResponse(valid=False, message="No client_version provided", server_version=server_ver)

        if client_ver == server_ver:
            return file_service_pb2.TestAuthResponse(valid=True, message="Cache Valid", server_version=server_ver)
        else:
            return file_service_pb2.TestAuthResponse(valid=False, message="Cache outdated", server_version=server_ver)
    
    def ListFiles(self, request, context):
        
        files = []
        self._require_caught_up(context)
        all_files = os.listdir(DATA_DIR)
        meta_filename = os.path.basename(META_PATH)
        start = request.offset
        end = start + request.limit if request.limit > 0 else len(all_files)

        for name in all_files[start:end]:
            # pending uploads and syncs live in .tmp files until committed
            if name == meta_filename or name.endswith(".tmp"):
                continue
            path = os.path.join(DATA_DIR, name)
            if os.path.isfile(path):
                files.append(file_service_pb2.FileTuple(
                    filename=name,
                    size=os.path.getsize(path)
                ))

        return file_service_pb2.ListFilesResponse(files=files)

    def ListSubdivisionFiles(self, request, context):
        files = []
        all_files = os.listdir(SUBDIVISIONS_DIR)
        meta_filename = os.path.basename(META_PATH)
        start = request.offset
        end = start + request.limit if request.limit > 0 else len(all_files)

        for name in all_files[start:end]:
            if name == meta_filename:
                continue
            path = os.path.join(SUBDIVISIONS_DIR, name)
            if os.path.isfile(path):
                files.append(file_service_pb2.FileTuple(
                    filename=name,
                    size=os.path.getsize(path)
                ))
        return file_service_pb2.ListSubdivisionFilesResponse(files=files)

    def RequestSubdivisions(self, request, context):
        os.makedirs(SUBDIVISIONS_DIR, exist_ok=True)

        size = request.subdivision_size
        all_files = os.listdir(DATA_DIR)
        for f in all_files:
            if "input" in f:
                path = os.path.join(DATA_DIR, f)
                if os.path.isfile(f):
                    self.divide_file(f, size)


        self._notify_peers(
            "ReplicateSubdivisions",
            file_service_pb2.ReplicateDivision(subdivision_size=size),
            timeout=10)


        return file_service_pb2.RequestDivisionResponse(success=True)

    def ReplicateSubdivisions(self, request, context):
        os.makedirs(SUBDIVISIONS_DIR, exist_ok=True)

        size = request.subdivision_size
        all_files = os.listdir(DATA_DIR)
        for f in all_files:
            if "input" in f:
                path = os.path.join(DATA_DIR, f)
                if os.path.isfile(f):
                    self.divide_file(f, size)
        return file_service_pb2.ReplicateDivisionResponse(success=True)


    #Proposition for file division function, a bit ugly, could be cleaned up.
    def divide_file(self, filename, subdivision_size):
        with open(os.path.join(DATA_DIR, filename), "rb") as f:
            main_file_lines = f.readlines()
            subdivision_count = math.ceil(len(main_file_lines) / subdivision_size)
            for i in range(subdivision_count):
                #sd{i} meaning subdivision of number 'i' e.g. inputfile_003_sd2.txt
                current_subdivision_filename = f"{filename.strip(".txt")}_sd{i}.txt"

                sd_file = open(os.path.join(SUBDIVISIONS_DIR, current_subdivision_filename), "wb")
                try:
                    sd_file.writelines(main_file_lines[i * subdivision_size :
                                                      (i + 1) * subdivision_size])
                except IndexError:
                    try:
                        leftover_lines = len(main_file_lines) % subdivision_size
                        sd_file.writelines(main_file_lines[i * subdivision_size :
                                                          i * subdivision_size + leftover_lines])
                    except:
                        sd_file.close()
                        os.remove(sd_file.name)
                        print("Failed to divide file")


                sd_file.close()

        print(f"Created {subdivision_count} subdivisions for file {filename}")

        #Clear up the subdivion files for particular original file, might as well clear them all at the end, but that is an easy change to make
        def cleanup_subdivisions(self, filename):
            for f in os.listdir(SUBDIVISIONS_DIR):
                #This part feels kinda ugly, there should be a better way to do this
                if f"{filename.strip(".txt")}" in f:
                    try:
                        sd_file = os.path.join(DATA_DIR, f)
                        os.remove(sd_file)
                    except OSError:
                        pass
            print(f"Cleaned up subdivisions for {filename}")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[('grpc.max_send_message_length', -1),('grpc.max_receive_message_length', -1),])

    serv = FileServiceServicer()
    file_service_pb2_grpc.add_FileServiceServicer_to_server(serv, server)

    port = int(RPC_PORT)
    server.add_insecure_port(f"0.0.0.0:{port}")
    print(f"[{SERVER_ID}] server started on port {port}, advertising {ADVERTISE_ADDR}, peers={PEERS}")
    server.start()
    # start raft timers after server is listening so RPCs don't race startup
    serv.raft.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
