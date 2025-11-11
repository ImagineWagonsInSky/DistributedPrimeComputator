import grpc
from concurrent import futures
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
import os, json, threading, uuid
import time, random
from .utils import DATA_DIR, META_PATH, file_timestamp, load_meta, save_meta
from .raft import RaftManager

SERVER_ID = os.environ.get(key="SERVER_ID", default="server-1")
PEERS = [p for p in os.environ.get("PEERS", "").split(",") if p]

class FileServiceServicer(file_service_pb2_grpc.FileServiceServicer):
    def __init__(self):
        self.meta = load_meta()

        self.lock = threading.RLock()

        self.server_id = SERVER_ID
        self.cluster_size = len(PEERS) + 1
        self.peers = PEERS[:]

        self.current_term = self.meta.get("term", 0)
        self.voted_for = self.meta.get("voted_for", None)

        # Added this so file_service_1 starts as leader
        is_leader_env = os.getenv("IS_LEADER", "false").lower() == "true"
        if is_leader_env:
            self.role = "leader"
        else:
            self.role = "follower"
        self.leader_host = os.getenv("LEADER_HOST") or self.meta.get("last_leader", None)
        self.last_heartbeat = time.time()

        # check the leader info when server-1 starts as leader by default
        if self.role == "leader":
            if not self.leader_host:
                new_host = os.getenv("RPC_HOST", "0.0.0.0")
                new_port = os.getenv("RPC_PORT", "50051")
                self.leader_host = f"{new_host}:{new_port}"
            self.meta["last_leader"] = self.leader_host
            self.meta["term"] = self.current_term
            save_meta(self.meta)
            print(f"[startup] {self.server_id} starting as pre-designated leader, advertising {self.leader_host}")
        
        self._cleanup_temp_files()

        # Move election and heartbeat behavior into a dedicated manager
        self.raft = RaftManager(self)


    def _cleanup_temp_files(self):
        """
        Remove any .tmp files that migt be left from crashed uploads
        """
        for filename in os.listdir(DATA_DIR):
            if filename.endswith('.tmp'):
                tmp_path = os.path.join(DATA_DIR, filename)
                os.remove(tmp_path)
                print(f"[startup] Cleaned up temp file: {filename}")

    def Heartbeat(self, request, context):
        with self.lock:
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
        
        print(f"[Leader] handling UploadFile for {filename}")

        # Write to temp file first, then rename in atomic way to avoid partial files
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
                os.remove(tmp_path)

            return file_service_pb2.UploadResponse(success=False, message=f"Local write failed: {e}")

        # 2. Replicate tmp to followers (so don't commit yet)
        acks = 1
        for peer in self.peers:
                channel = grpc.insecure_channel(peer)
                stub = file_service_pb2_grpc.FileServiceStub(channel)
                resp = stub.ReplicateFile(
                    file_service_pb2.ReplicateRequest(filename=filename, data=data, request_id=request_id), timeout=5)
                if resp.success:
                    acks += 1

        majority = (self.cluster_size // 2) + 1
        if acks < majority:
            os.remove(tmp_path)
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
                channel = grpc.insecure_channel(peer)
                stub = file_service_pb2_grpc.FileServiceStub(channel)
                stub.CleanupTemp(file_service_pb2.CleanupTempRequest(filename=filename, request_id=request_id), timeout=1)
            return file_service_pb2.UploadResponse(success=False, message=f"Commit failed locally: {e}")
    
        # 4. Tell followers to commit their tmp in best-effort manner
        for peer in self.peers:
            channel = grpc.insecure_channel(peer)
            stub = file_service_pb2_grpc.FileServiceStub(channel)
            stub.CommitFile(file_service_pb2.CommitRequest(filename=filename, request_id=request_id), timeout=2)

        return file_service_pb2.UploadResponse(success=True, message="Uploaded and replicated (committed)")

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
                os.remove(tmp_path)
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
            
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

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
    threading.Thread(target=serv.raft.election_timer, daemon=True).start()
    # if this node was pre-designated as leader via environment, start heartbeats now
    if serv.role == 'leader':
        threading.Thread(target=serv.raft.send_heartbeats, daemon=True).start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
