from time import time
import grpc
import threading
from proto.generated.coordinator import coordinator_pb2, coordinator_pb2_grpc
import google.protobuf.empty_pb2
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
from concurrent import futures
from math import ceil
import os
import argparse
import uuid
import pickle

from services.file_service import client

parser = argparse.ArgumentParser()
parser.add_argument("--cache-dir", default="services/coordinator/coordinator_cache")
args = parser.parse_args()

CACHE_DIR = args.cache_dir
os.makedirs(CACHE_DIR, exist_ok=True)

class CoordinatorServicer(coordinator_pb2_grpc.CoordinatorServicer):
    OUTPUT_PATH = os.path.join(CACHE_DIR, "primes.txt")
    SNAPSHOT_PATH = os.path.join(CACHE_DIR, "snapshot.pkl")
    CHUNK_SIZE = 10000
    
    def __init__(self):
        # --- FILESYSTEM STUB SETUP ---
        filesystem_host = os.getenv("FILE_SERVICE_HOST", "localhost")
        filesystem_port = os.getenv("FILE_SERVICE_PORT", "50051")

        self.worker_last_heartbeat = {}
        self.HEARTBEAT_TIMEOUT = 5.0

        filesystem_channel = grpc.insecure_channel(
            f"{filesystem_host}:{filesystem_port}", 
            options=[
                ('grpc.max_send_message_length', -1),
                ('grpc.max_receive_message_length', -1),
            ])
        self.filesystem_stub = file_service_pb2_grpc.FileServiceStub(filesystem_channel)

        # --- DEDUPLICATION STATE ---
        # Initialize in-memory set to track all unique primes found so far.
        self.found_primes = set()
        self.primes_lock = threading.Lock()

        # --- CACHE-AWARE SCHEDULER STATE ---
        # A dictionary of lists that holds a separate queue of tasks for each file.
        self.task_queues = {}
        # A dictionary that tracks the last file assigned to each worker.
        self.worker_affinity = {}
        self.tasks_in_progress = {}
        self.active_workers = set()
        self.task_queues_lock = threading.Lock()

        self.local_cache_dir = CACHE_DIR

        # --- SNAPSHOT STATE ---
        self.current_snapshot_id = None
        # stores snapshot as we build it 
        self.pending_snapshot = {}
        # Which workers have sent their snapshot chunk
        self.workers_in_snapshot = {}
        self.snapshot_lock = threading.Lock()

        if not self._load_latest_snapshot():
            print("No snapshot found, populating the fresh queue")    
            self._populate_queues()
        print("Coordinator initialized")
        self._initiate_snapshot()
        self._start_snapshot_timer()
        self._start_heartbeat_monitor()

    def _populate_queues(self):
        """
        Populate work queue with all files on fileserver seperatee into CHUNK_SIZE pieces
        """
        self.task_queues = {}
        # If the files have reasonable size
        filename_list = self.filesystem_stub.ListFiles(file_service_pb2.ListFilesRequest())

        # if (self.check_subdivision_need(filename_list)):
        #     # Wait for subdivisions to get created, not sure how to do that yet
        #     filename_list = self.filesystem_stub.ListSubdivisionFiles(
        #         file_service_pb2.ListSubdivisionFilesRequest()) # Supposed to be a list of subdivisions files instead of a normal list of files
        task_counter = 0
        
        for f in filename_list.files:
            if not f.filename == "primes.txt":
                self.task_queues[f.filename] = []

                for i in range(ceil(f.size / self.CHUNK_SIZE)):
                    task_id = f"task_{task_counter}"
                    task_counter += 1
                    task = (task_id, f.filename, i * self.CHUNK_SIZE, self.CHUNK_SIZE)
                    self.task_queues[f.filename].append(task)

    def GetWork(self, request, context):
        """
        Sends a task to the worker
        """
        with self.task_queues_lock:
            
            # Use worker_id to fetch the preferred input file
            worker_id = request.worker_id
            preferred_file = self.worker_affinity.get(worker_id)
            task_to_assign = None 

            if preferred_file and self.task_queues.get(preferred_file):
                task_to_assign = self.task_queues[preferred_file].pop(0)

                task_id, filename, start_line, num_lines = task_to_assign
                self.tasks_in_progress[task_id] = (task_to_assign, worker_id)
                # If this was the last task for the file, clear the queue
                if not self.task_queues[preferred_file]:
                    del self.task_queues[preferred_file]

                return coordinator_pb2.WorkResponse(
                    no_more_work=False,
                    filename=filename,
                    start_line=start_line,
                    num_lines=num_lines,
                    task_id = task_id,
                    snapshot_id = self.current_snapshot_id
                )
            
            # Cache miss, no preferred task so find a new file with remaining tasks
            if not task_to_assign:
                for filename, task_list in self.task_queues.items():
                    if task_list:
                        task_to_assign = task_list.pop(0)
                        self.worker_affinity[worker_id] = filename
                        
                        # If that was the last task for the file, clear the queue
                        if not task_list:
                            del self.task_queues[filename]
                        break
        
            # Work was found, now assign it
            if task_to_assign:
                task_id, filename, start_line, num_lines = task_to_assign
                self.tasks_in_progress[task_id] = (task_to_assign, worker_id)

                return coordinator_pb2.WorkResponse(
                    no_more_work=False,
                    filename=filename,
                    start_line=start_line,
                    num_lines=num_lines,
                    snapshot_id = self.current_snapshot_id,
                    task_id = task_id
                )
            
            return coordinator_pb2.WorkResponse(no_more_work=True, filename="", start_line=0, num_lines=0, task_id = "", snapshot_id = self.current_snapshot_id)

    def SubmitPrimeBatch(self, request, context):
        """
        Adds a batch of primes from a Worker to found_primes and write to primes.txt
        """
        worker_id = request.worker_id

        # Record channel if the snapshot is active and we are currently recording messages from this worker (== "PENDING") 
        with self.snapshot_lock:
            if self.current_snapshot_id and self.workers_in_snapshot.get(worker_id) == "PENDING":
                self.pending_snapshot["in_flight_messages"][worker_id].append(request)

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

        with self.task_queues_lock:
            if request.task_id in self.tasks_in_progress:
                del self.tasks_in_progress[request.task_id]

        return coordinator_pb2.SubmitBatchResponse(snapshot_id = self.current_snapshot_id)
    
    def _start_snapshot_timer(self):
        """
        Timer that initiate snapshots every 30 seconds
        """
        # Start a snapshot every 30 seconds
        threading.Timer(30.0, self._initiate_snapshot).start()

    def _initiate_snapshot(self):
        """
        Records the coordinator state and starts recording all incoming channels.
        """
        with self.snapshot_lock:
            if self.current_snapshot_id:
                print("A snapshot is already in progress.")
                self._start_snapshot_timer()
                return
            
            print("INTIATING GLOBAL SNAPSHOT")
            self.current_snapshot_id = str(uuid.uuid4())

            with self.task_queues_lock:
                with self.primes_lock:
                    coordinator_state = {
                        "task_queues": self.task_queues,
                        "worker_affinity": self.worker_affinity,
                        "tasks_in_progress": self.tasks_in_progress,
                        "found_primes": self.found_primes
                    }
        
            self.pending_snapshot = {
                "coordinator_state": pickle.dumps(coordinator_state),
                "worker_states": {},
                "in_flight_messages": {}
            }

            with self.task_queues_lock:
                active_worker_ids = list(self.active_workers)

            self.workers_in_snapshot = {wid: "PENDING" for wid in active_worker_ids}
            for wid in active_worker_ids:
                self.pending_snapshot["in_flight_messages"][wid] = []
        # Reschedule the next timer
        self._start_snapshot_timer()

    def SubmitSnapshotChunk(self, request, context):
        """
        Records worker state. If all worker states have been recorded saves the snapshot to fileserver
        """
        with self.snapshot_lock:
            # ignore old snapshot
            if request.snapshot_id != self.current_snapshot_id:
                return google.protobuf.empty_pb2.Empty()
            
            self.workers_in_snapshot[request.worker_id] = "DONE"
            self.pending_snapshot["worker_states"][request.worker_id] = request.process_state

            if all(status == "DONE" for status in self.workers_in_snapshot.values()):
                # Global snapshot complete, save to filesystem
                try:
                    self._save_snapshot_to_fs(self.pending_snapshot)
                except Exception as e:
                    print(f"Failed to save snapshot to fileserver: {e}")
                
                self.current_snapshot_id = None
                self.pending_snapshot = {}
                self.workers_in_snapshot = {}
        
        return google.protobuf.empty_pb2.Empty()
    
    def _save_snapshot_to_fs(self, snapshot_data):
        """
        Overwrites previous snapshot and saves it to fileserver
        """

        print("Saving the global snapshot to fileserver")
        try:
            snapshot_bytes = pickle.dumps(snapshot_data)

            client.write_bytes_to_local(self.SNAPSHOT_PATH, snapshot_bytes)

            client.close_file(self.filesystem_stub, self.SNAPSHOT_PATH)

        except grpc.RpcError as e:
            print(f"gRPC error when saving snapshot: {e.details()}")
    
    def _load_latest_snapshot(self):
        """
        If a snapshot exists on the fileserver, it unpickles it and restores the coordinator state from that.
        """
        try:
            local_file, _ = client.open_or_validate(self.filesystem_stub, self.SNAPSHOT_PATH) 

            if local_file is None:
                print("No snapshot file found on server")
                return False

            with open(local_file, "rb") as f:
                snapshot_bytes = f.read()
            
            snapshot_data = pickle.loads(snapshot_bytes)

            # Restore coordintator state
            coordinator_state = pickle.loads(snapshot_data["coordinator_state"])
            with self.task_queues_lock:
                self.task_queues = coordinator_state["task_queues"]
                self.worker_affinity = coordinator_state["worker_affinity"]
                self.tasks_in_progress = coordinator_state["tasks_in_progress"]
                self.found_primes = coordinator_state["found_primes"]
            
            # Rebuild primes.txt from found_primes
            try:
                # Completely overwrite current primes.txt file
                with open(self.OUTPUT_PATH, "w") as f:
                    for prime in self.found_primes:
                        f.write(f"{prime}\n")
                
                client.close_file(self.filesystem_stub, self.OUTPUT_PATH)
                print("primes.txt has been restored on fileserver")
            
            except grpc.RpcError as e:
                print(f"gRPC error when restoring primes.txt on fileserver: {e.details()}")

            # Requeue all tasks that were in progress
            with self.task_queues_lock:
                for task_id, (task_tuple, worker_id) in self.tasks_in_progress.items():
                    filename = task_tuple[1]
                    if filename not in self.task_queues:
                        self.task_queues[filename] = []
                    self.task_queues[filename].insert(0, task_tuple)

                self.tasks_in_progress.clear()

            # Reprocess all in flight messages
            for worker_id, messages in snapshot_data["in_flight_messages"].items():
                for msg in messages:
                    self.SubmitPrimeBatch(msg, None)

            print("SNAPSHOT LOADED SUCCESSFULLY")
            return True

        except grpc.RpcError as e:
            print(f"Failed to load snapshot: {e.details()}.")
            return False

    def request_file_division(self, subdivision_size):
        return file_service_pb2.RequestDivision(subdivision_size=subdivision_size)

    def check_subdivision_need(self, file_list):
        maximum_size = self.CHUNK_SIZE * 1000
        optimal_size = self.CHUNK_SIZE * 100
        if(len(file_list.files) < 8):
            self.request_file_division(optimal_size)
            return True
        for f in file_list.files:
            detected_size = len(f.readlines())
            if detected_size > maximum_size:
                self.request_file_division(optimal_size)
                return True
        return False
    
    def Heartbeat(self, request_iterator, context):
        worker_id = None
        try:
            for heartbeat in request_iterator:
                worker_id = heartbeat.worker_id
                self._handle_heartbeat(worker_id)
        except grpc.RpcError as e:
            pass  # Will log in finally block based on task state
        finally:
            if worker_id:
                with self.task_queues_lock:
                    # Requeue any tasks this worker had in progress
                    tasks_to_requeue = [
                        (tid, task_tuple)
                        for tid, (task_tuple, assigned_wid) in list(self.tasks_in_progress.items())
                        if assigned_wid == worker_id
                    ]

                    if tasks_to_requeue:
                        print(f"Worker {worker_id} CRASHED with {len(tasks_to_requeue)} incomplete tasks - requeuing for reassignment")
                        for tid, task_tuple in tasks_to_requeue:
                            filename = task_tuple[1]
                            if filename not in self.task_queues:
                                self.task_queues[filename] = []
                            self.task_queues[filename].insert(0, task_tuple)
                            del self.tasks_in_progress[tid]
                    else:
                        print(f"Worker {worker_id} disconnected (completed all assigned work)")

                    self.active_workers.discard(worker_id)
                    if worker_id in self.worker_last_heartbeat:
                        del self.worker_last_heartbeat[worker_id]
                    if worker_id in self.worker_affinity:
                        del self.worker_affinity[worker_id]
        return google.protobuf.empty_pb2.Empty()
    
    def _handle_heartbeat(self, worker_id):
        if not worker_id:
            return
        current_time = time()
        with self.task_queues_lock:
            self.worker_last_heartbeat[worker_id] = current_time
            self.active_workers.add(worker_id)

    def _check_worker_timeouts(self):
        current_time = time()
        with self.task_queues_lock:
            failed_workers = []
            for wid, last_seen in list(self.worker_last_heartbeat.items()):
                if current_time - last_seen > self.HEARTBEAT_TIMEOUT:
                    print(f"Worker {wid} timeout! Last seen {current_time - last_seen:.1f}s ago")
                    failed_workers.append(wid)

            for wid in failed_workers:
                tasks_to_requeue = [
                    (tid, task_tuple)
                    for tid, (task_tuple, assigned_wid) in list(self.tasks_in_progress.items())
                    if assigned_wid == wid
                ]

                if tasks_to_requeue:
                    print(f"Worker {wid} timeout with {len(tasks_to_requeue)} tasks, requeuing...")

                for tid, task_tuple in tasks_to_requeue:
                    filename = task_tuple[1]
                    if filename not in self.task_queues:
                        self.task_queues[filename] = []
                    self.task_queues[filename].insert(0, task_tuple)
                    del self.tasks_in_progress[tid]

                self.active_workers.discard(wid)
                self.worker_last_heartbeat.pop(wid, None)
                self.worker_affinity.pop(wid, None)

    def _start_heartbeat_monitor(self):
        threading.Timer(0.5, self._periodic_heartbeat_check).start()

    def _periodic_heartbeat_check(self):
        self._check_worker_timeouts()
        self._start_heartbeat_monitor()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    coordinator_pb2_grpc.add_CoordinatorServicer_to_server(CoordinatorServicer(), server)
    server.add_insecure_port("0.0.0.0:50052")
    print("Coordinator Server listening on port 50052...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
