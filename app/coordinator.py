import grpc
import threading
import coordinator_pb2, coordinator_pb2_grpc
import google.protobuf.empty_pb2
from client import client, file_service_pb2, file_service_pb2_grpc
from concurrent import futures
from math import ceil

class CoordinatorServicer(coordinator_pb2_grpc.CoordinatorServicer):
    OUTPUT_FILENAME = "primes.txt"
    CHUNK_SIZE = 1000
    
    def __init__(self, cache_dir):
        filesystem_channel = grpc.insecure_channel("localhost:50051")
        self.filesystem_stub = file_service_pb2_grpc.FileServiceStub(filesystem_channel)

        # Initialize in-memory set to track all unique primes found so far.
        self.found_primes = set()
        self.primes_lock = threading.lock()

        # Work queue with elements (filename, start_line, num_lines)
        self.work_queue = [] 
        self._populate_queue()
        self.work_queue_lock = threading.lock()

        self.local_cache_dir = cache_dir
        print("Coordinator initialized")
    
    def _populate_queue(self):
        """
        Populate work queue with all files on fileserver seperatee into CHUNK_SIZE pieces
        """
        # hardcoded for now
        filename_list = [("input_dataset_001", 200000), ("input_dataset_002", 400000), ("input_dataset_003", 600000)]
        # BUT Fileserver RPC would return list of tuples (filename, file_size), or should it do less/more processing?
        # filename_list = client.list_files(self.filesystem_stub)
        for file_name, size in filename_list:
            for i in range(ceil(size / self.CHUNK_SIZE)):
                self.work_queue.append((file_name, i * self.CHUNK_SIZE, self.CHUNK_SIZE))

    def GetWork(self, request, context):
        #Currently requests isn't used because it is empty. Would be useful in future for tracking who is processing what chunks?
        with self.work_queue_lock:
            if not self.work_queue:
                return coordinator_pb2.WorkResponse(no_more_work=True, filename="", start_line=0, num_lines=0)
            
            filename, start_line, num_lines = self.work_queue.pop(0)
        return coordinator_pb2.WorkResponse(no_more_work=False, filename=filename, start_line=start_line, num_lines=num_lines)

    def SubmitPrimeBatch(self, request, context):
        with self.primes_lock:
            new_primes = []
            for prime in request.primes:
                if prime not in self.found_primes:
                    new_primes.append(prime)
                    self.found_primes.add(prime)

            if new_primes:
                print(f"Added new prime batch: {new_primes}")
                try:
                    # add all primes batch to output file
                    # unsure if this is the correct way to use the client methods
                    local_file = client.open_file(self.filesystem_stub, self.OUTPUT_FILENAME)
                    client.write_local(local_file, new_primes)
                    client.close_file(self.filesystem_stub, self.OUTPUT_FILENAME)
                
                except grpc.RpcError as e:
                    print(f"gRPC error: {e.details}")
                    for p in new_primes:
                        self.found_primes.remove(p)

        return google.protobuf.empty_pb2.Empty()
    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    # no clue what cache dir should be called
    cache_dir="tmp/coordinator-cache"

    coordinator_pb2_grpc.add_CoordinatorServicer_to_server(CoordinatorServicer(cache_dir=cache_dir), server)
    
    server.add_insecure_port("[::]:50052")
    print("Coordinator Server listening on port 50052...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()