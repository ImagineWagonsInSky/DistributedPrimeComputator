import grpc
import threading
from proto.generated.coordinator import coordinator_pb2, coordinator_pb2_grpc
import google.protobuf.empty_pb2
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
from concurrent import futures
from math import ceil
import os
import argparse

from services.file_service import client

parser = argparse.ArgumentParser()
parser.add_argument("--cache-dir", default="services/coordinator/coordinator_cache")
args = parser.parse_args()

CACHE_DIR = args.cache_dir
os.makedirs(CACHE_DIR, exist_ok=True)

class CoordinatorServicer(coordinator_pb2_grpc.CoordinatorServicer):
    OUTPUT_PATH = os.path.join(CACHE_DIR, "primes.txt")
    CHUNK_SIZE = 1000
    
    def __init__(self):
        filesystem_channel = grpc.insecure_channel("localhost:50051", options=[('grpc.max_send_message_length', -1),('grpc.max_receive_message_length', -1),])
        self.filesystem_stub = file_service_pb2_grpc.FileServiceStub(filesystem_channel)

        # Initialize in-memory set to track all unique primes found so far.
        self.found_primes = set()
        self.primes_lock = threading.Lock()

        # A dictionary of lists that holds a separate queue of tasks for each file.
        self.task_queues = {}
        # A dictionary that tracks the last file assigned to each worker.
        self.worker_affinity = {}
        self._populate_queues()
        self.task_queues_lock = threading.Lock()

        self.local_cache_dir = CACHE_DIR
        print("Coordinator initialized")
    
    def _populate_queues(self):
        """
        Populate work queue with all files on fileserver seperatee into CHUNK_SIZE pieces
        """
        self.task_queues = {}
        
        filename_list = self.filesystem_stub.ListFiles(file_service_pb2.ListFilesRequest())
        for f in filename_list.files:
            if not f.filename == "primes.txt":
                self.task_queues[f.filename] = []

                for i in range(ceil(f.size / self.CHUNK_SIZE)):
                    task = (f.filename, i * self.CHUNK_SIZE, self.CHUNK_SIZE)
                    self.task_queues[f.filename].append(task)

    def GetWork(self, request, context):
        with self.task_queues_lock:
            
            # Use worker_id to fetch the preferred input file
            worker_id = request.worker_id
            preferred_file = self.worker_affinity.get(worker_id)

            if preferred_file and self.task_queues.get(preferred_file):
                filename, start_line, num_lines = self.task_queues[preferred_file].pop(0)    

                # If this was the last task for the file, clear the queue
                if not self.task_queues[preferred_file]:
                    del self.task_queues[preferred_file]

                return coordinator_pb2.WorkResponse(no_more_work=False, filename=filename, start_line=start_line, num_lines=num_lines)
            
            # Cache miss, no preferred task so find a new file with remaining tasks
            for f, task_list in self.task_queues.items():
                if task_list:

                    filename, start_line, num_lines = task_list.pop(0)
                                        
                    # If that was the last task for the file, clear the queue
                    if not task_list:
                        del self.task_queues[f]

                    self.worker_affinity[worker_id] = filename

                    return coordinator_pb2.WorkResponse(no_more_work=False, filename=filename, start_line=start_line, num_lines=num_lines)
                        
            return coordinator_pb2.WorkResponse(no_more_work=True, filename="", start_line=0, num_lines=0)

    def SubmitPrimeBatch(self, request, context):
        with self.primes_lock:
            new_primes = []
            for prime in request.primes:
                if prime not in self.found_primes:
                    new_primes.append(prime)
                    self.found_primes.add(prime)

            if new_primes:
                try:
                    # add all primes batch to output file
                    # unsure if this is the correct way to use the client methods
                    local_path, _ = client.open_or_validate(self.filesystem_stub, self.OUTPUT_PATH)
                    client.write_primes_to_local(local_path,new_primes)                    
                    client.close_file(self.filesystem_stub, self.OUTPUT_PATH)
                
                except grpc.RpcError as e:
                    print(f"gRPC error: {e.details}")
                    for p in new_primes:
                        self.found_primes.remove(p)

        return google.protobuf.empty_pb2.Empty()
    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # no clue what cache dir should be called
    # cache_dir="tmp/coordinator-cache"
    # coordinator_pb2_grpc.add_CoordinatorServicer_to_server(CoordinatorServicer(cache_dir=cache_dir), server)

    coordinator_pb2_grpc.add_CoordinatorServicer_to_server(CoordinatorServicer(), server)
    
    server.add_insecure_port("[::]:50052")
    print("Coordinator Server listening on port 50052...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()