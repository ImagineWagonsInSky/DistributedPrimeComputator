import grpc
from proto.generated.coordinator import coordinator_pb2, coordinator_pb2_grpc
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
from prime_testing import prime_testing
import time
import argparse
import os
import google.protobuf.empty_pb2

from services.file_service import client

# Can add a different cache directory to test cache validation on independent clients
parser = argparse.ArgumentParser()
parser.add_argument("--cache-dir", default="services/coordinator/client1_cache")
args = parser.parse_args()

CACHE_DIR = args.cache_dir
os.makedirs(CACHE_DIR, exist_ok=True)

class Worker():
    def __init__(self):
        coordinator_channel = grpc.insecure_channel('localhost:50052')
        self.coordinator_stub = coordinator_pb2_grpc.CoordinatorStub(coordinator_channel)

        filesystem_channel = grpc.insecure_channel("localhost:50051")
        self.filesystem_stub = file_service_pb2_grpc.FileServiceStub(filesystem_channel)    

        self.current_task = None
        self.local_cache_dir = CACHE_DIR

    def run(self):
        """The main processing loop for the worker."""
        print("Worker starting up...")
        while True:
            try:
                work_reponse = self.coordinator_stub.GetWork(google.protobuf.empty_pb2.Empty())

                if work_reponse.no_more_work:
                    print("No more chunks. Worker exiting.")
                    break
                
                self.current_task = work_reponse

                prime_batch = self._process_task(self.current_task)

                if prime_batch:
                    submit_req = coordinator_pb2.SubmitBatchRequest(primes=prime_batch)
                    self.coordinator_stub.SubmitPrimeBatch(submit_req)

                self.current_task = None

            except grpc.RpcError as e:
                print(f"gRPC Error: {e.details()}. Retrying in 2 seconds...")
                time.sleep(2) 

    
    def _process_task(self, task):
        """Handles primality testing"""
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

        return primes_found

if __name__ == "__main__":
    worker = Worker()
    worker.run()