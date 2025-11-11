import grpc
from proto.generated.coordinator import coordinator_pb2, coordinator_pb2_grpc
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
from prime_testing import prime_testing
import time
import argparse
import os
import pickle
import uuid
from services.file_service import client

parser = argparse.ArgumentParser()
parser.add_argument("--cache-dir", default="services/coordinator/client1_cache")
args = parser.parse_args()

CACHE_DIR = args.cache_dir
os.makedirs(CACHE_DIR, exist_ok=True)

class Worker():
    def __init__(self):
        # --- FILESYSTEM and COORDINATOR STUB SETUP ---
        # Get host/port from environment or fall back to defaults for local testing
        coordinator_host = os.getenv("COORDINATOR_HOST", "localhost")
        coordinator_port = os.getenv("COORDINATOR_PORT", "50052")

        file_service_host = os.getenv("FILE_SERVICE_HOST", "localhost")
        file_service_port = os.getenv("FILE_SERVICE_PORT", "50051")

        # Connect to coordinator
        coordinator_channel = grpc.insecure_channel(
            f"{coordinator_host}:{coordinator_port}",
            options=[
                ("grpc.max_send_message_length", -1),
                ("grpc.max_receive_message_length", -1),
            ],
        )
        self.coordinator_stub = coordinator_pb2_grpc.CoordinatorStub(coordinator_channel)

        # Connect to file service
        filesystem_channel = grpc.insecure_channel(
            f"{file_service_host}:{file_service_port}",
            options=[
                ("grpc.max_send_message_length", -1),
                ("grpc.max_receive_message_length", -1),
            ],
        )
        self.filesystem_stub = file_service_pb2_grpc.FileServiceStub(filesystem_channel)

        self.current_task = None
        self.local_cache_dir = CACHE_DIR

        # --- SCHEDULING AND SNAPSHOT --- 
        self.worker_id = str(uuid.uuid4())
        self.last_snapshot_id = None
        print(f"Worker starting up with ID: {self.worker_id}")

        # --- PRESERVING CHUNKS ---
        self.preserved_chunks = {}

    def run(self):
        """
        The main processing loop for the worker.
        """
        while True:
            try:
                work_request = coordinator_pb2.GetWorkRequest(worker_id = self.worker_id)
                work_reponse = self.coordinator_stub.GetWork(work_request)

                # Handle potential snapshot marker
                self._handle_snapshot_marker(work_reponse.snapshot_id)
                
                if work_reponse.no_more_work:
                    print("No more chunks. Worker exiting.")
                    break
                
                self.current_task = work_reponse

                prime_batch = self._process_task(self.current_task)

                submit_req = coordinator_pb2.SubmitBatchRequest(
                    primes=prime_batch,
                    task_id = self.current_task.task_id,
                    worker_id = self.worker_id
                )
                submit_response = self.coordinator_stub.SubmitPrimeBatch(submit_req)

                # Handle potential snapshot marker
                self._handle_snapshot_marker(submit_response.snapshot_id)
                self.current_task = None

            except grpc.RpcError as e:
                print(f"gRPC Error: {e.details()}. Retrying in 2 seconds...")
                time.sleep(2) 

    def _process_task(self, task):
        """
        Handles primality testing
        """
        print(f"Processing chunk: {task.filename}...")
        
        primes_found = []
        try:
            local_path = os.path.join(self.local_cache_dir, task.filename)
            local_file, _ = client.open_or_validate(self.filesystem_stub, local_path) 
        except grpc.RpcError as e:
                print(f"gRPC Error when opening remote file: {e.details()}.")

        start = task.start_line
        end = task.start_line + task.num_lines
        
        try:
            with open(local_file, 'r') as f:
                for i, line in enumerate(f):
                    if i < start:
                        continue
                    
                    if i >= end:
                        break
                    
                    number = int(line.strip())
                    # currently just using deterministic but should be easy to change
                    if prime_testing.miller_rabin_deterministic(number):
                        primes_found.append(number)
                        
        except FileNotFoundError:
            print(f"Error: Local cache file not found at {local_file}")
            return []
        
        finally:
            try:
                client.close_file(self.filesystem_stub, local_path)
            except grpc.RpcError as e:
                print(f"gRPC error when trying to close file: {e.details()}")

        self.preserved_chunks.update({"Task" : task, "preserved_primes" : primes_found})
        return primes_found

    def _handle_snapshot_marker(self, snapshot_id):
        """
        Checks if the marker is new, if it is save worker state and send to coordinator.
        """
        if snapshot_id and snapshot_id != self.last_snapshot_id:
            print(f"Received marker: {snapshot_id}")
            self.last_snapshot_id = snapshot_id

            # Save worker state (current task)
            try:
                task_data = None
                if self.current_task:
                    task_data = {
                        "task_id": self.current_task.task_id,
                        "filename": self.current_task.filename,
                        "start_line": self.current_task.start_line,
                        "num_lines": self.current_task.num_lines
                    }

                state_bytes = pickle.dumps(task_data)

            except Exception as e:
                print(f"Error when pickling worker state: {e}")
                state_bytes = pickle.dumps(None)
            
            # Send snapshot to Coordinator
            try:
                chunk = coordinator_pb2.SnapshotChunk(
                    snapshot_id = snapshot_id,
                    worker_id = self.worker_id, 
                    process_state = state_bytes
                )
                self.coordinator_stub.SubmitSnapshotChunk(chunk)
                print(f"Submitted snapshot chunk for {snapshot_id}")
            except grpc.RpcError as e:
                print(f"Error when submitting snapshot chunk: {e.details()}")

            self.preserved_chunks.clear()

    def RecoverPreservedChunks(self):
        for i in self.preserved_chunks:
            submit_req = coordinator_pb2.SubmitBatchRequest(
                primes=i["preserved_primes"],
                task_id=i["Task"].task_id,
                worker_id=self.worker_id
            )
            submit_response = self.coordinator_stub.SubmitPrimeBatch(submit_req)

            # Handle potential snapshot marker
            self._handle_snapshot_marker(submit_response.snapshot_id)

        return None

    def HeartbeatResponse(self):
        return None

if __name__ == "__main__":
    worker = Worker()
    worker.run()