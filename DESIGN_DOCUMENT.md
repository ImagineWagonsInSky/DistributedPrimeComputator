# Distributed Prime Number Finder with Fault-Tolerant File System
## Design Document

**Course:** Distributed Systems
**System Type:** Coordinator-Worker Architecture with Replicated File Service
**Implementation:** Python 3, gRPC, Docker Compose

---

## 1. Introduction

### 1.1 System Overview

This system implements a distributed prime number finder that processes large datasets containing numerical values and identifies all prime numbers. The architecture consists of three primary components operating in a coordinated manner:

1. **Replicated File Service**: A fault-tolerant distributed file system implementing Raft-based consensus for data replication and consistency.
2. **Coordinator**: A central orchestrator responsible for task distribution, result aggregation, and distributed snapshot coordination.
3. **Workers**: Computational nodes that perform primality testing on assigned data chunks using the Miller-Rabin deterministic algorithm.

The system is containerized using Docker and communicates via gRPC remote procedure calls, enabling language-agnostic, high-performance inter-service communication.

### 1.2 Design Goals

The primary objectives of this distributed system are:

- **Fault Tolerance**: Survive and recover from arbitrary component failures including file servers, coordinator crashes, and worker failures without data loss or duplicate computation.
- **Scalability**: Support horizontal scaling by adding workers to increase computational throughput proportional to workload.
- **Consistency**: Ensure exactly-once semantics for prime number discovery with no duplicates across concurrent workers.
- **Availability**: Maintain system operation despite minority replica failures through quorum-based replication.
- **Performance**: Optimize task distribution through cache-aware scheduling to minimize redundant file transfers.

### 1.3 Key Assumptions

- Input datasets are pre-loaded into the file service before system initialization.
- All replicas are initialized with identical file sets at startup.
- The underlying storage medium is reliable (no disk corruption).
- Network partitions are transient and eventually resolve.
- The system operates within a single data center with reasonable network latency.

---

## 2. System Architecture

### 2.1 Infrastructure Components

The system deployment consists of the following containerized services:

**File Service Cluster (4 replicas)**:
- `file_service_1` (port 50051): Initial leader replica
- `file_service_2` (port 50054): Follower replica
- `file_service_3` (port 50053): Follower replica
- `file_service_4` (port 50055): Follower replica

Each file service node implements the Raft consensus protocol for leader election and log replication. The cluster can tolerate up to 1 failure while maintaining availability (f = (n-1)/2 where n=4, thus f=1).

**Coordinator Service (1 instance)**:
- Port 50052
- Connects to file service leader for persistent storage
- Maintains ephemeral state (task queues, worker registry, prime set)
- Initiates distributed snapshots every 30 seconds

**Worker Service (scalable)**:
- Dynamically scaled via Docker Compose `--scale worker=N`
- Each worker connects to both coordinator and file service
- Maintains local file cache with timestamp validation
- Stateless computation with snapshot-based recovery

### 2.2 Communication Protocols

All inter-component communication uses gRPC with Protocol Buffers for efficient binary serialization. The system defines three proto service interfaces:

**FileService RPCs**:
- `OpenFile(filename)`: Retrieve file contents with timestamp
- `UploadFile(filename, data)`: Write file with quorum replication
- `TestAuth(filename, client_timestamp)`: Validate cache freshness
- `ReplicateFile(filename, data, request_id)`: Leader-to-follower replication
- `CommitFile(filename, request_id)`: Two-phase commit finalization
- `Heartbeat()`: Raft leader heartbeat for failure detection
- `RequestVote()`: Raft election protocol

**Coordinator RPCs**:
- `GetWork(worker_id)`: Request task assignment with cache affinity
- `SubmitPrimeBatch(primes, task_id, worker_id)`: Return discovered primes
- `SubmitSnapshotChunk(worker_id, process_state, snapshot_id)`: Chandy-Lamport snapshot contribution
- `HeartBeat(worker_id)`: Worker liveness signal (streaming RPC)

### 2.3 Data Structures

**Coordinator State**:
```python
task_queues: Dict[filename, List[Task]]          # Per-file task queues
worker_affinity: Dict[worker_id, filename]       # Cache-aware scheduling
tasks_in_progress: Dict[task_id, (Task, worker)] # Failure recovery tracking
found_primes: Set[int]                           # Deduplication set
active_workers: Set[worker_id]                   # Liveness tracking
```

**Worker State**:
```python
current_task: Task                               # Active computation
local_cache_dir: Path                            # File cache location
coordinator_stub: gRPC stub                      # RPC client
filesystem_stub: gRPC stub                       # RPC client
```

**File Service State**:
```python
meta: Dict[filename, {size, timestamp}]          # File metadata
current_term: int                                # Raft term number
voted_for: Optional[server_id]                   # Raft vote tracking
is_leader: bool                                  # Leadership status
peers: List[host:port]                           # Replica addresses
```

---

## 3. Distributed File System Design

### 3.1 Replication Protocol

The file service implements a simplified Raft consensus protocol with the following characteristics:

**Leader Election**:
- Each server starts as a follower with a randomized election timeout (150-300ms).
- If no heartbeat received within timeout, follower transitions to candidate state.
- Candidate increments `current_term`, votes for itself, sends `RequestVote` to all peers.
- Server grants vote if: (1) candidate's term ≥ server's term, (2) server hasn't voted in this term.
- Candidate becomes leader upon receiving majority votes.
- New leader immediately sends heartbeats to establish authority.

**Quorum-Based Writes (Two-Phase Commit)**:
1. **Prepare Phase**: Leader writes data to temporary file `{filename}.{request_id}.tmp`, flushes to disk with `fsync()`.
2. **Replicate Phase**: Leader sends `ReplicateFile` RPC to all followers with temporary file data.
3. **Quorum Wait**: Leader blocks until receiving acknowledgments from majority (⌈n/2⌉ including itself).
4. **Commit Phase**: Leader atomically renames temp file to final filename using `os.replace()` (atomic syscall).
5. **Commit Broadcast**: Leader sends best-effort `CommitFile` RPC to followers to finalize their replicas.

**Failure Handling**:
- If majority quorum not achieved, leader aborts write and sends `CleanupTemp` RPC to followers.
- If leader crashes after local commit but before followers commit, followers eventually sync during recovery.
- Atomic rename ensures no partial writes visible to readers.

### 3.2 Client Fault Tolerance

**Read Retry Mechanism**:
```python
def _rpc_retry(call_fn, retries=7, backoff=1.0, retry_codes={UNAVAILABLE, DEADLINE_EXCEEDED}):
    for attempt in range(retries):
        try:
            return call_fn()
        except grpc.RpcError as e:
            if e.code() not in retry_codes or attempt >= retries - 1:
                raise
            time.sleep(backoff * (2 ** attempt))  # Exponential backoff
```

**Cache Validation**:
- Clients store local copies with associated server timestamps.
- Before reusing cache, client calls `TestAuth(filename, cached_timestamp)`.
- Server responds `valid=True` if timestamp matches, otherwise `valid=False`.
- Invalid cache triggers full refetch via `OpenFile`.

**Leader Discovery**:
- Clients initially connect to `file_service_1` (default leader).
- If contacted server is not leader, response includes `NOT_LEADER` message with `leader_host` hint.
- Client automatically redirects request to hinted leader.
- If hint fails, client iterates through all peers from `FILE_SERVICE_PEERS` environment variable.

### 3.3 Atomic Write Guarantees

All write operations follow atomic commit pattern:

1. **Client Side**: Downloads write to `.tmp` file, only renames to final name after complete transmission.
2. **Server Side**: Writes to `{filename}.{uuid}.tmp`, replicates to majority, commits via atomic rename.
3. **Cleanup Protocol**: Server startup scans for orphaned `.tmp` files and removes them.

This design ensures:
- No partial file contents visible to readers.
- Client crashes during write leave no corrupt data on server.
- Server crashes during write either complete fully or leave no trace.

---

## 4. Coordinator-Worker Architecture

### 4.1 Task Distribution

**Work Queue Initialization**:
```python
CHUNK_SIZE = 10000  # Lines per task

for file in filesystem.ListFiles():
    if file.filename != "primes.txt":
        num_chunks = ceil(file.size / CHUNK_SIZE)
        for i in range(num_chunks):
            task = Task(
                task_id=f"task_{counter}",
                filename=file.filename,
                start_line=i * CHUNK_SIZE,
                num_lines=CHUNK_SIZE
            )
            task_queues[file.filename].append(task)
```

**Cache-Aware Scheduling**:
```python
def GetWork(worker_id):
    preferred_file = worker_affinity[worker_id]

    # Prioritize tasks from worker's cached file
    if preferred_file in task_queues and task_queues[preferred_file]:
        task = task_queues[preferred_file].pop(0)
        tasks_in_progress[task.task_id] = (task, worker_id)
        return task

    # Cache miss: assign from any available file
    for filename, tasks in task_queues.items():
        if tasks:
            task = tasks.pop(0)
            worker_affinity[worker_id] = filename
            tasks_in_progress[task.task_id] = (task, worker_id)
            return task

    return NO_MORE_WORK
```

This affinity-based scheduling minimizes redundant file downloads when workers request consecutive tasks from the same input file.

### 4.2 Prime Discovery Algorithm

Workers employ the Miller-Rabin deterministic primality test:

```python
def miller_rabin_deterministic(n):
    if n < 2:
        return False

    # Write n-1 as 2^r * d
    r, d = 0, n - 1
    while d % 2 == 0:
        d //= 2
        r += 1

    # Test against predetermined witness bases
    bases = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37}
    for a in bases:
        if n == a:
            return True
        if is_composite(n, a, d, r):
            return False

    return True
```

**Time Complexity**: O(k log³ n) where k is the number of witness bases (k=12).
**Correctness**: Deterministic for all n < 2^64, no probabilistic false positives.

### 4.3 Result Aggregation and Deduplication

**Prime Submission Protocol**:
```python
def SubmitPrimeBatch(request):
    worker_id = request.worker_id

    with primes_lock:  # Thread-safe critical section
        new_primes = []
        for prime in request.primes:
            if prime not in found_primes:  # O(1) set lookup
                new_primes.append(prime)
                found_primes.add(prime)

        if new_primes:
            # Append to output file
            local_path = open_or_validate(filesystem_stub, OUTPUT_PATH)
            with open(local_path, 'a') as f:
                for p in new_primes:
                    f.write(f"{p}\n")
            close_file(filesystem_stub, OUTPUT_PATH)

    # Remove completed task from in-progress tracking
    del tasks_in_progress[request.task_id]
```

**Deduplication Guarantee**: The in-memory `found_primes` set ensures O(1) duplicate detection. Combined with thread-safe locking, this guarantees exactly-once output for each prime across all workers.

---

## 5. Fault Tolerance and Recovery

### 5.1 Chandy-Lamport Distributed Snapshot

The coordinator implements the Chandy-Lamport algorithm for consistent global state capture:

**Snapshot Initiation (every 30 seconds)**:
```python
def _initiate_snapshot():
    snapshot_id = uuid.uuid4()

    # Record local state atomically
    with task_queues_lock, primes_lock:
        coordinator_state = {
            'task_queues': deepcopy(task_queues),
            'worker_affinity': deepcopy(worker_affinity),
            'tasks_in_progress': deepcopy(tasks_in_progress),
            'found_primes': deepcopy(found_primes)
        }

    # Initialize channel recording for all active workers
    for worker_id in active_workers:
        workers_in_snapshot[worker_id] = "PENDING"
        pending_snapshot['in_flight_messages'][worker_id] = []

    # Broadcast snapshot markers via GetWork responses
    current_snapshot_id = snapshot_id
```

**Channel Message Recording**:
- All `SubmitPrimeBatch` messages arriving after snapshot initiation are recorded in `in_flight_messages[worker_id]`.
- Recording stops when worker sends `SubmitSnapshotChunk` (indicates it received marker).
- This captures all messages "in transit" on the logical channel.

**Worker Snapshot Participation**:
```python
def run():
    while True:
        work_response = coordinator_stub.GetWork(worker_id)

        if work_response.snapshot_id and not acknowledged_snapshot:
            # Received snapshot marker
            snapshot_chunk = {
                'worker_id': worker_id,
                'current_task': current_task,
                'cached_files': list_cached_files()
            }
            coordinator_stub.SubmitSnapshotChunk(snapshot_chunk)
            acknowledged_snapshot = True
```

**Snapshot Finalization**:
- Coordinator waits until all workers in `workers_in_snapshot` reach status "DONE".
- Combines coordinator state + all worker states + all in-flight messages.
- Serializes entire snapshot using pickle and uploads to file service via `UploadFile("snapshot.pkl")`.

**Recovery Protocol**:
```python
def _load_latest_snapshot():
    snapshot_data = pickle.loads(download("snapshot.pkl"))

    # Restore coordinator state
    coordinator_state = pickle.loads(snapshot_data['coordinator_state'])
    self.task_queues = coordinator_state['task_queues']
    self.worker_affinity = coordinator_state['worker_affinity']
    self.tasks_in_progress = coordinator_state['tasks_in_progress']
    self.found_primes = coordinator_state['found_primes']

    # Rebuild primes.txt from found_primes set
    with open(OUTPUT_PATH, 'w') as f:
        for prime in self.found_primes:
            f.write(f"{prime}\n")
    upload(OUTPUT_PATH)

    # Requeue all in-progress tasks (worker may have failed)
    for task_id, (task, worker_id) in tasks_in_progress.items():
        task_queues[task.filename].insert(0, task)
    tasks_in_progress.clear()

    # Replay all in-flight messages
    for worker_id, messages in snapshot_data['in_flight_messages'].items():
        for msg in messages:
            SubmitPrimeBatch(msg, None)
```

**Correctness Guarantee**: The Chandy-Lamport algorithm ensures the recorded snapshot represents a consistent global state (one that could have occurred during the distributed computation). Replaying in-flight messages prevents lost updates.

### 5.2 Worker Failure Handling

**Current Implementation**:
- When coordinator restarts from snapshot, all `tasks_in_progress` are requeued.
- Failed worker's tasks are automatically reassigned to healthy workers.
- Duplicate primes from recomputation are filtered by `found_primes` set.

**Limitation**: Active worker failure detection via heartbeat is defined but not fully implemented. The `_handle_heartbeat()` method exists as a stub (line 369-372 in coordinator.py) but does not perform timeout-based failure detection or task reallocation during runtime.

### 5.3 File Service Recovery

**Follower Recovery**:
- When crashed follower restarts, it sends heartbeat responses to current leader.
- Leader detects follower is behind (implementation gap: lacks log-based catch-up).
- Follower receives future writes via `ReplicateFile` RPCs and gradually syncs.

**Leader Recovery**:
- When crashed leader restarts, it discovers new leader via `RequestVote` responses.
- Reverts to follower state and accepts new leader's authority.
- Clients automatically discover new leader via NOT_LEADER redirects.

---

## 6. Performance Characteristics

### 6.1 Scalability Analysis

**Worker Scaling**:
- System scales horizontally by adding workers: `docker-compose up --scale worker=N`.
- Theoretical speedup: S(N) = T₁/Tₙ where T₁ is single-worker time.
- Expected speedup limited by: (1) Coordinator bottleneck (single instance), (2) File service bandwidth, (3) Task granularity.

**Chunk Size Trade-off**:
- Small chunks (< 1000 lines): High coordination overhead, frequent RPC calls.
- Large chunks (> 100000 lines): Poor load balancing, stragglers delay completion.
- Current setting (10000 lines): Balances RPC overhead with work distribution.

### 6.2 Bottleneck Analysis

**Coordinator**: Single instance processes all GetWork and SubmitPrimeBatch requests. Under high worker count (N > 100), coordinator becomes CPU-bound on lock contention for `found_primes` set.

**File Service Leader**: All writes funnel through leader. Leader bandwidth limits write throughput to primes.txt during high discovery rate.

**Network**: Worker-to-FileService OpenFile RPCs transfer large datasets. Cache hit rate critical for performance (measured via worker affinity effectiveness).

### 6.3 Fault Tolerance Overhead

**Snapshot Cost**:
- 30-second interval trades off recovery granularity vs. overhead.
- Snapshot serialization: O(|found_primes| + |task_queues|) ≈ O(P + C) where P is primes discovered, C is chunks remaining.
- Upload to file service: O(snapshot_size) typically 1-10 MB.

**Replication Cost**:
- Each write requires majority acknowledgment (3/4 servers with n=4).
- Latency: Twrite = Tlocal + Treplicate + Tcommit ≈ 2-5ms in LAN.
- Throughput: Limited by leader network bandwidth and disk fsync rate.

---

## 7. Testing Strategy

### 7.1 Functional Tests

1. **Single Worker, Single File**: Verify correct prime identification on small dataset (10 numbers).
2. **Multiple Workers, Deduplication**: 3 workers process 3 files, confirm no duplicate primes in output.
3. **Server Crash During Read**: Kill file service during OpenFile, verify client retries and succeeds.
4. **Client Crash During Write**: Kill worker during SubmitPrimeBatch, verify no partial data on server.
5. **Replication Verification**: Write primes.txt, check all 4 replicas contain identical data.
6. **Server Failure During Write**: Kill leader during UploadFile, verify client redirects and write completes.
7. **Coordinator Failure**: Kill coordinator mid-computation, restart, verify resumes from snapshot.

### 7.2 Performance Tests

1. **Throughput Scaling**: Measure completion time with 1, 2, 4, 8 workers on identical dataset.
2. **Large Dataset**: Process 1 million numbers, verify correctness and no memory exhaustion.
3. **Cache Hit Rate**: Measure worker affinity effectiveness via OpenFile RPC count.

### 7.3 Expected Results

Based on implementation analysis:
- ✅ Tests 1-7: Expected to pass (core functionality implemented).
- ⚠️ Worker failure during runtime: May not detect/reassign immediately (heartbeat incomplete).
- ⚠️ File service recovery sync: Slow convergence (lacks explicit log catch-up).

---

## 8. Limitations and Future Work

### 8.1 Known Limitations

1. **Single Coordinator Bottleneck**: No coordinator replication, single point of failure between snapshots.
2. **Passive Worker Failure Detection**: Heartbeat infrastructure exists but lacks timeout-based detection and runtime task reassignment.
3. **File Service Log Replication**: Raft implementation lacks AppendEntries log synchronization for efficient recovery.
4. **No Dynamic Load Balancing**: Fixed chunk size and static affinity may cause stragglers on skewed data distributions.
5. **Memory-Bound Processing**: Workers load entire chunks into memory; unsuitable for extremely large per-line data.

### 8.2 Potential Improvements

1. **Coordinator Replication**: Implement Raft consensus among multiple coordinators for high availability.
2. **Active Worker Monitoring**: Complete heartbeat implementation with configurable timeout (e.g., 10s) and automatic task reallocation.
3. **Raft Log Synchronization**: Add persistent log structure and AppendEntries RPC for efficient follower catch-up.
4. **Adaptive Chunk Sizing**: Dynamically adjust CHUNK_SIZE based on worker performance metrics.
5. **Streaming Processing**: Implement line-by-line streaming to reduce memory footprint on large files.
6. **Subdivision Activation**: Complete and activate the existing file subdivision code for datasets exceeding worker memory capacity.

---

## 9. Conclusion

This distributed prime number finder demonstrates key distributed systems concepts including fault-tolerant replication, consistent global snapshots, and cache-aware scheduling. The system successfully addresses the core requirements:

- **Survivability**: Tolerates minority file service failures (f=1 for n=4), coordinator crashes (via snapshots), and worker failures (via task requeuing).
- **Recoverability**: Coordinator restores full state from Chandy-Lamport snapshots including in-flight messages.
- **Delivery**: Guarantees exactly-once prime output through thread-safe deduplication.
- **Scalability**: Supports horizontal worker scaling with cache-aware task assignment.
- **Performance**: Implements retry mechanisms, atomic writes, and efficient RPC protocols.

The architecture balances simplicity with fault tolerance, making pragmatic design choices (simplified Raft, snapshot-based recovery) suitable for the computational workload while maintaining correctness guarantees. Future work should focus on completing active failure detection and log-based synchronization for production readiness.

---

## References

1. Chandy, K. M., & Lamport, L. (1985). Distributed snapshots: Determining global states of distributed systems. *ACM Transactions on Computer Systems*, 3(1), 63-75.
2. Ongaro, D., & Ousterhout, J. (2014). In search of an understandable consensus algorithm. *USENIX ATC*, 305-319.
3. Rabin, M. O. (1980). Probabilistic algorithm for testing primality. *Journal of Number Theory*, 12(1), 128-138.
4. Google. (2023). gRPC: A high-performance, open source universal RPC framework. https://grpc.io/
5. Liskov, B., & Cowling, J. (2012). Viewstamped replication revisited. *MIT Technical Report*.
