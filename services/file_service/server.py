import grpc
from concurrent import futures
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
import os, json, threading, uuid
import time, random

DATA_DIR = "services/file_service/server_store" # we could do /DATA in containers?
os.makedirs(DATA_DIR, exist_ok=True)

SERVER_ID = os.environ.get(key="SERVER_ID", default="server-1")
PEERS = [p for p in os.environ.get("PEERS", "").split(",") if p]

# temp for testing
ELECTION_TIMEOUT = (3.0, 7.0)
HEARTBEAT_INTERVAL = 1.0

META_PATH = os.path.join(DATA_DIR, "meta.json")
TMP_SUFFIX = ".tmp"


def file_timestamp(path):
    return int(os.path.getmtime(path))

def load_meta():
    if not os.path.exists(META_PATH):
        meta = {"term": 0, "voted_for": None, "last_leader": None, "file_sizes": {}}
        save_meta(meta)
        return meta
    with open(META_PATH, "r") as f:
        return json.load(f)


def save_meta(meta):
    tmp = META_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(meta, f)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, META_PATH)

class FileServiceServicer(file_service_pb2_grpc.FileServiceServicer):
    def __init__(self):
        self.meta = load_meta()

        self.lock = threading.RLock() # RLock in case of self deadlock

        self.server_id = SERVER_ID
        self.cluster_size = len(PEERS) + 1
        self.peers = PEERS[:]

        # Raft-like state
        self.current_term = self.meta.get("term", 0)
        self.voted_for = self.meta.get("voted_for", None)
        # Allow a pre-designated leader via environment variable at startup
        is_leader_env = os.getenv("IS_LEADER", "false").lower() == "true"
        self.role = "leader" if is_leader_env else "follower"
        # If a leader host was provided explicitly via env, prefer that for redirects
        self.leader_host = os.getenv("LEADER_HOST") or self.meta.get("last_leader", None)
        self.last_heartbeat = time.time()

        self._cleanup_temp_files()
        # Persist initial leader info when starting as designated leader
        if self.role == "leader":
            # if leader_host not set, advertise this node's host:port
            if not self.leader_host:
                new_host = os.getenv("RPC_HOST", "0.0.0.0")
                new_port = os.getenv("RPC_PORT", "50051")
                self.leader_host = f"{new_host}:{new_port}"
            self.meta["last_leader"] = self.leader_host
            self.meta["term"] = self.current_term
            save_meta(self.meta)
            print(f"[startup] {self.server_id} starting as pre-designated leader, advertising {self.leader_host}")

    def _cleanup_temp_files(self):
        """Remove any .tmp files left from crashed uploads"""
        for filename in os.listdir(DATA_DIR):
            if filename.endswith('.tmp'):
                tmp_path = os.path.join(DATA_DIR, filename)
                try:
                    os.remove(tmp_path)
                    print(f"[startup] Cleaned up temp file: {filename}")
                except OSError:
                    pass

    # -----------------------
    # Heartbeat & Election RPCs
    # -----------------------
    def Heartbeat(self, request, context):
        with self.lock:
            # print(f"[{self.server_id}] received heartbeat from {request.leader_id} in term {request.term}")
            if request.term >= self.current_term:
                # If heartbeat has equal or higher term, accept it and step down
                self.role = "follower"
                # If the heartbeat term is higher, clear any voted_for state from older terms
                if request.term > self.current_term:
                    self.voted_for = None
                self.current_term = request.term
                self.leader_host = request.leader_id
                self.last_heartbeat = time.time()

                self.meta["term"] = self.current_term
                self.meta["last_leader"] = self.leader_host
                self.meta["voted_for"] = self.voted_for
                save_meta(self.meta)
                return file_service_pb2.HeartbeatResponse(ok=True, term=self.current_term)
            return file_service_pb2.HeartbeatResponse(ok=False, term=self.current_term)

    def RequestVote(self, request, context):
        with self.lock:
            # If the request's term is older, deny immediately
            if request.term < self.current_term:
                return file_service_pb2.VoteResponse(vote_granted=False, term=self.current_term)

            # If the request has a higher term, adopt it and clear any previous vote
            if request.term > self.current_term:
                self.current_term = request.term
                self.voted_for = None
                self.meta["term"] = self.current_term
                self.meta["voted_for"] = self.voted_for
                save_meta(self.meta)

            # Grant vote if we haven't voted yet in this term or we already voted for this candidate
            if self.voted_for in (None, request.candidate_id):
                self.voted_for = request.candidate_id
                self.meta["voted_for"] = self.voted_for
                save_meta(self.meta)
                print(f"[Election] Voted for {request.candidate_id} in term {self.current_term}")
                return file_service_pb2.VoteResponse(vote_granted=True, term=self.current_term)

            # Otherwise deny vote
            return file_service_pb2.VoteResponse(vote_granted=False, term=self.current_term)

    # -----------------------
    # Election Loop
    # -----------------------
    def election_timer(self):
        # small startup jitter to avoid all nodes starting elections at the same moment
        initial_jitter = random.uniform(0.0, 1.0)
        time.sleep(initial_jitter)
        while True:
            timeout = random.uniform(*ELECTION_TIMEOUT)
            start = time.time()

            while time.time() - start < timeout:
                with self.lock:
                    if time.time() - self.last_heartbeat < timeout:
                        break
                time.sleep(0.05)
            else:
                # timeout expired and no heartbeat -> start election
                with self.lock:
                    if self.role == "leader":
                        continue
                    self.start_election()

    def start_election(self):
        # Start a new election term and request votes from peers.
        with self.lock:
            self.current_term += 1
            self.role = "candidate"
            self.voted_for = self.server_id
            self.meta["term"] = self.current_term
            self.meta["voted_for"] = self.voted_for
            save_meta(self.meta)
            term = self.current_term

        votes = 1  # vote for self
        print(f"[Election] {self.server_id} starting election for term {term}")

        majority = (self.cluster_size // 2) + 1

        # Contact peers in parallel but bound how long we wait for all peers so
        # that a single down peer doesn't block the election indefinitely.
        def contact_peer(p):
            # Try a few quick attempts to contact a peer and return its VoteResponse or None
            for attempt in range(2):
                try:
                    ch = grpc.insecure_channel(p)
                    try:
                        grpc.channel_ready_future(ch).result(timeout=0.8)
                    except Exception:
                        # peer not ready yet for this attempt
                        # small backoff and retry
                        time.sleep(0.05 + random.uniform(0, 0.05))
                        continue
                    stub = file_service_pb2_grpc.FileServiceStub(ch)
                    try:
                        return stub.RequestVote(file_service_pb2.VoteRequest(term=term, candidate_id=self.server_id), timeout=1.5)
                    except Exception:
                        # RPC-level error for this attempt
                        time.sleep(0.05)
                        continue
                except Exception:
                    time.sleep(0.05)
                    continue
            # After bounded attempts, give up on this peer for this election
            return None

        # Launch RPCs
        futures_map = {}
        with futures.ThreadPoolExecutor(max_workers=max(1, len(self.peers))) as exc:
            for peer in self.peers:
                futures_map[exc.submit(contact_peer, peer)] = peer

            # Wait a bounded amount of time for the peer RPCs to complete
            try:
                done, not_done = futures.wait(list(futures_map.keys()), timeout=3)
            except Exception as e:
                done = set()
                not_done = set(futures_map.keys())

            # Process completed futures
            remaining_possible = len(self.peers)
            for fut in done:
                peer = futures_map.get(fut)
                remaining_possible -= 1
                try:
                    resp = fut.result()
                except Exception as e:
                    print(f"[Election] contacting {peer} raised: {e}")
                    resp = None

                if resp is None:
                    print(f"[Election] no response from {peer}")
                else:
                    # Peer responded; check term and vote
                    if getattr(resp, 'term', None) is not None and resp.term > term:
                        with self.lock:
                            self.current_term = resp.term
                            self.role = 'follower'
                            self.voted_for = None
                            self.meta['term'] = self.current_term
                            self.meta['voted_for'] = self.voted_for
                            save_meta(self.meta)
                        print(f"[Election] stepping down: peer {peer} has higher term {resp.term}")
                        return
                    if resp.vote_granted:
                        votes += 1
                        print(f"[Election] received vote from {peer} (term={resp.term})")
                    else:
                        print(f"[Election] vote denied by {peer} (term={resp.term})")

                # Early exit: if we've reached majority, we can stop
                if votes >= majority:
                    break

            # Any not-yet-done futures are considered unreachable for this election
            for fut in not_done:
                peer = futures_map.get(fut)
                remaining_possible -= 1
                # best-effort cancel
                try:
                    fut.cancel()
                except Exception:
                    pass
                print(f"[Election] peer {peer} did not respond in time and will be treated as unreachable")

        # Final decision based on votes received from reachable nodes
        with self.lock:
            if votes >= majority:
                self.role = "leader"
                new_host = os.getenv("RPC_HOST", "0.0.0.0")
                new_port = os.getenv("RPC_PORT", "50051")
                self.leader_host = f"{new_host}:{new_port}"
                self.meta["last_leader"] = self.leader_host
                save_meta(self.meta)
                print(f"[Leader] {self.server_id} WON elected leader (term {term})")
                threading.Thread(target=self.send_heartbeats, daemon=True).start()
            else:
                print(f"[Election] {self.server_id} lost election (votes={votes})")
                self.role = "follower"

    def send_heartbeats(self):
        while True:
            with self.lock:
                if self.role != "leader":
                    break
                term = self.current_term
                leader_id = self.leader_host or self.server_id
            for peer in self.peers:
                try:
                    # print(f"[Leader:{self.server_id}] sending heartbeat to {peer} (term={term})")
                    channel = grpc.insecure_channel(peer)
                    stub = file_service_pb2_grpc.FileServiceStub(channel)
                    stub.Heartbeat(file_service_pb2.HeartbeatRequest(
                        leader_id=leader_id, term=term
                    ))
                except Exception as e:
                    print(f"[Leader:{self.server_id}] heartbeat to {peer} failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL)


    def CreateFile(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        if os.path.exists(path):
            return file_service_pb2.CreateResponse(success=False, message="File already exists")
        with open(path, "wb") as f:
            f.write(request.data)
        return file_service_pb2.CreateResponse(success=True, message="File created")

    def OpenFile(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        if not os.path.exists(path):
            return file_service_pb2.OpenResponse(success=False, message="No such file")
        with open(path, "rb") as f:
            data = f.read()
        ts = file_timestamp(path)
        return file_service_pb2.OpenResponse(success=True, data=data, message="File sent", server_timestamp=ts)
    '''
    def UploadFile(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        with open(path, "wb") as f:
            f.write(request.data)
        return file_service_pb2.UploadResponse(success=True, message="File uploaded successfully")
    '''

    def UploadFile(self, request, context):
        filename = request.filename
        data = request.data

        # CASE 1: If not leader, redirect to client
        with self.lock:
            if self.role != "leader":
                hint = self.leader_host or ""
                print(f"Redirecting UploadFile to leader {hint}")
                return file_service_pb2.UploadResponse(
                    success=False,
                    message="NOT_LEADER",
                    leader_host = hint
                )
        
        # CASE 2: Leader handles the write + replication
        print(f"[Leader] handling UploadFile for {filename}")

        # ATOMIC WRITE PATTERN
        # Write to temp file first, then atomically rename to avoid partial files
        request_id = str(int(time.time() * 1000)) + "-" + uuid.uuid4().hex[:8]
        tmp_name = f"{filename}.{request_id}.tmp"
        tmp_path = os.path.join(DATA_DIR, tmp_name)

        # 1. Write local tmp
        try:
            with open(tmp_path, "wb") as f:
                f.write(data)
                f.flush(); os.fsync(f.fileno())
        except Exception as e:
            if os.path.exists(tmp_path):
                try: os.remove(tmp_path)
                except Exception: pass
            return file_service_pb2.UploadResponse(success=False, message=f"Local write failed: {e}")

        # 2. Replicate tmp to followers (so don't commit yet)
        acks = 1
        for peer in self.peers:
            try:
                channel = grpc.insecure_channel(peer)
                stub = file_service_pb2_grpc.FileServiceStub(channel)
                resp = stub.ReplicateFile(
                    file_service_pb2.ReplicateRequest(filename=filename, data=data, request_id=request_id, timeout=5)
                )
                if resp.success:
                    acks += 1
            except Exception:
                pass
        
        majority = (self.cluster_size // 2) + 1
        if acks < majority:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            return file_service_pb2.UploadResponse(
                success=False,
                message=f"Failed to replicate {filename} to majority"
            )

        # 3. Commit locally
        final_path = os.path.join(DATA_DIR, filename)
        try:
            os.replace(tmp_path, final_path)
            with self.lock:
                self.meta["file_sizes"][filename] = os.path.getsize(final_path)
                save_meta(self.meta)
        except Exception as e:
            # tell followers to cleanup if we can't commit locally
            for peer in self.peers:
                try:
                    channel = grpc.insecure_channel(peer)
                    stub = file_service_pb2_grpc.FileServiceStub(channel)
                    stub.CleanupTemp(file_service_pb2.CleanupTempRequest(filename=filename, request_id=request_id), timeout=1)
                except Exception:
                    pass
            return file_service_pb2.UploadResponse(success=False, message=f"Commit failed locally: {e}")
    
        # 4. Tell followers to commit their tmp in best-effort manner
        for peer in self.peers:
            try:
                channel = grpc.insecure_channel(peer)
                stub = file_service_pb2_grpc.FileServiceStub(channel)
                stub.CommitFile(file_service_pb2.CommitRequest(filename=filename, request_id=request_id), timeout=2)
            except Exception:
                pass
        return file_service_pb2.UploadResponse(success=True, message="Uploaded and replicated (committed)")

    # -------------------------
    # Follower RPCs for replication lifecycle
    # -------------------------
    def ReplicateFile(self, request, context):
        """
        Follower replica receives file data from leader and stores it locally to a tmp file.
        """
        filename = request.filename
        request_id = getattr(request, "request_id", None) or str(int(time.time() * 1000))
        data = request.data
        tmp_name = f"{filename}.{request_id}.tmp"
        tmp_path = os.path.join(DATA_DIR, tmp_name)

        try:
            with open(tmp_path, "wb") as f:
                f.write(data)
                f.flush(); os.fsync(f.fileno())
            return file_service_pb2.UploadResponse(success=True, message="tmp stored")
        except Exception as e:
            if os.path.exists(tmp_path):
                try: os.remove(tmp_path)
                except Exception: pass
            print(f"[Follower] Replication error: {e}")
            return file_service_pb2.UploadResponse(success=False, message=str(e))

    def CommitFile(self, request, context):
        """
        Rename stored tmp -> final (commit) on follower.
        """
        filename = request.filename
        request_id = getattr(request, "request_id", None)
        tmp_name = f"{filename}.{request_id}.tmp" if request_id else None
        tmp_path = os.path.join(DATA_DIR, tmp_name) if tmp_name else None
        final_path = os.path.join(DATA_DIR, filename)

        if not tmp_path or not os.path.exists(tmp_path):
            # nothing to commit
            return file_service_pb2.CommitResponse(success=False, message="no tmp")
        try:
            os.replace(tmp_path, final_path)
            with self.lock:
                self.meta["file_sizes"][filename] = os.path.getsize(final_path)
                save_meta(self.meta)
            return file_service_pb2.CommitResponse(success=True, message="committed")
        except Exception as e:
            return file_service_pb2.CommitResponse(success=False, message=str(e))

    def CleanupTemp(self, request, context):
        request_id = getattr(request, "request_id", None)
        filename = request.filename
        
        if request_id:
            tmp_name = f"{filename}.{request_id}.tmp"
            tmp_path = os.path.join(DATA_DIR, tmp_name)
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
        return file_service_pb2.CleanupTempResponse(ok=True, message="cleaned")

    def TestAuth(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        if not os.path.exists(path):
            return file_service_pb2.TestAuthResponse(valid=False, message="File not found", server_timestamp=0)
        server_ts = file_timestamp(path)
        if server_ts == request.client_timestamp:
            return file_service_pb2.TestAuthResponse(valid=True, message="Cache Valid", server_timestamp=server_ts)
        else: 
            return file_service_pb2.TestAuthResponse(valid=False, message="Cache outdated", server_timestamp=server_ts)
    
    def ListFiles(self, request, context):
        """
        Returns repeated list of tuples:
        (filename: string, size: uint64)
        Optional offset and file limit
        """
        files = []
        all_files = os.listdir(DATA_DIR)
        meta_filename = os.path.basename(META_PATH)
        start = request.offset
        end = start + request.limit if request.limit > 0 else len(all_files)

        for name in all_files[start:end]:
            if name == meta_filename:
                continue
            path = os.path.join(DATA_DIR, name)
            if os.path.isfile(path):
                files.append(file_service_pb2.FileTuple(
                    filename=name,
                    size=os.path.getsize(path)
                ))

        return file_service_pb2.ListFilesResponse(files=files)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[('grpc.max_send_message_length', -1),('grpc.max_receive_message_length', -1),])

    serv = FileServiceServicer()
    file_service_pb2_grpc.add_FileServiceServicer_to_server(serv, server)

    port = int(os.environ.get("RPC_PORT", "50051"))
    server.add_insecure_port(f"0.0.0.0:{port}")
    print(f"[{SERVER_ID}] server started on port {port}, peers={PEERS}")
    server.start()
    # start election timer after server is listening so RPCs don't race startup
    threading.Thread(target=serv.election_timer, daemon=True).start()
    # if this node was pre-designated as leader via environment, start heartbeats now
    if serv.role == 'leader':
        threading.Thread(target=serv.send_heartbeats, daemon=True).start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
