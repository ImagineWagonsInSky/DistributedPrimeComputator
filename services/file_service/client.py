import grpc
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
import os
import threading
import time

MAX_RETRY_BACKOFF = 4.0
# How long close_file keeps retrying while there is no leader. Must outlast an election
# (3-7s election timeout plus a round of voting), e.g. at startup or after the leader dies.
UPLOAD_RETRY_WINDOW = 20.0
# Deadline for calls through FailoverStub, so a hung server fails over instead of blocking forever
FAILOVER_RPC_TIMEOUT = 15.0
FAILOVER_CODES = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)

CHANNEL_OPTIONS = [
    ("grpc.max_send_message_length", -1),
    ("grpc.max_receive_message_length", -1),
    ("grpc.initial_reconnect_backoff_ms", 500),
    ("grpc.min_reconnect_backoff_ms", 500),
    ("grpc.max_reconnect_backoff_ms", 2000),
    ("grpc.dns_min_time_between_resolutions_ms", 500),
]


class FailoverStub:
    """
    Drop-in replacement for FileServiceStub that knows every file server.
    Calls go to the current server; if it is unreachable, the call moves on to the next
    one in the list and that server becomes the new current one.
    """

    def __init__(self, addrs, rpc_timeout=FAILOVER_RPC_TIMEOUT):
        self._addrs = addrs
        self._stubs = [file_service_pb2_grpc.FileServiceStub(grpc.insecure_channel(a, options=CHANNEL_OPTIONS))
                       for a in addrs]
        self._current = 0
        self._lock = threading.Lock()
        self._rpc_timeout = rpc_timeout

    def __getattr__(self, method):
        def call(request, timeout=None, **kwargs):
            last_err = None
            for _ in range(len(self._stubs)):
                idx = self._current
                try:
                    return getattr(self._stubs[idx], method)(request, timeout=timeout or self._rpc_timeout, **kwargs)
                except grpc.RpcError as e:
                    if e.code() not in FAILOVER_CODES:
                        raise
                    last_err = e
                    with self._lock:
                        if self._current == idx:
                            self._current = (idx + 1) % len(self._stubs)
                            print(f"[failover] {self._addrs[idx]} unreachable ({e.code().name}), "
                                  f"switching to {self._addrs[self._current]}")
            raise last_err
        return call


def make_file_service_stub():
    """FailoverStub over FILE_SERVICE_HOST:PORT first, then the rest of FILE_SERVICE_PEERS."""
    primary = f"{os.getenv('FILE_SERVICE_HOST', 'localhost')}:{os.getenv('FILE_SERVICE_PORT', '50051')}"
    peers = [p for p in os.getenv("FILE_SERVICE_PEERS", "").split(",") if p]
    return FailoverStub([primary] + [p for p in peers if p != primary])


def _rpc_retry(call_fn, *args, retries=7, backoff=1.0, retry_codes=None, **kwargs):
    """
    Generic RPC retry helper.
    Retries on UNAVAILABLE / DEADLINE_EXCEEDED by default.
    """
    if retry_codes is None:
        retry_codes = (
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
        )

    attempt = 0
    while True:
        try:
            return call_fn(*args, **kwargs)
        except grpc.RpcError as e:
            attempt += 1
            code = e.code()
            if attempt >= retries or code not in retry_codes:
                print(f"[rpc_retry] RPC failed, no more retries")
                raise
            sleep_for = min(backoff * (2 ** (attempt - 1)), MAX_RETRY_BACKOFF)
            print(f"[rpc_retry] RPC {code}, retrying in {sleep_for:.1f}s (attempt {attempt}/{retries})...")
            time.sleep(sleep_for)
    
def validate(filename, stub, cached_ts):
    print(f"Found cached copy of '{filename}' with version={cached_ts}, validating...")
    try:
        resp = _rpc_retry(
            stub.TestAuth,
            file_service_pb2.TestAuthRequest(filename=filename, client_version=int(cached_ts)),
        )
    except grpc.RpcError:
        print("Cache validation failed after retries; fetching fresh copy.")
        return False
    
    if resp.valid:
        print("Cache valid, using local copy.")
        return True
    else:
        print("Cache outdated, refetching from server.")
        return False

def open_or_validate(stub, local_path, max_retries=7):
    """
    Get a local, up-to-date copy of the file.

    - if cached copy + .ts exists, validate with server
    - else download from server
    - download writes to <file>.tmp first
    - only after full successful download, rename tmp -> real
    - remote download is wrapped in RPC retry so server crash during read is tolerated
    """
    filename = os.path.basename(local_path)
    ts_file = local_path + ".ts"

    if os.path.exists(local_path) and os.path.exists(ts_file):
        with open(ts_file) as f:
            cached_ts = int(f.read().strip())
        if validate(filename=filename, stub=stub, cached_ts=cached_ts):
            return local_path, cached_ts
    else:
        print(f"No cached copy for {filename}, fetching from server...")

    tmp_path = local_path + ".tmp"

    try:
        grpc_start_time = time.time()
        print(f"[TIMER] Pure gRPC OpenFile call STARTED for '{filename}'")

        # Manual Test Pause for Server Crash
        #print(f"\n[TEST MODE] Pausing for 15 seconds. Kill File Server NOW")
        #for i in range(15, 0, -1):
        #    print(f"[TEST MODE] OpenFile RPC starts in {i} seconds")
        #    time.sleep(1)
        #print(f"[TEST MODE] Starting OpenFile RPC, Kill File Server to test retry!")

        resp = _rpc_retry(
            stub.OpenFile,
            file_service_pb2.OpenRequest(filename=filename),
            retries=max_retries,
            backoff=1.0,
        )

        grpc_end_time = time.time()
        grpc_duration = grpc_end_time - grpc_start_time
        #print(f"[TIMER] Pure gRPC OpenFile call ended, Duration: {grpc_duration:.3f}s (includes retries)")

    except grpc.RpcError:
        print(f"[open_or_validate] OpenFile RPC failed after retries, will retry.")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        return None, None

    if not resp.success:
        print(f"[open_or_validate] OpenFile failed from server: {resp.message}")
        return None, None

    with open(tmp_path, "wb") as f:
        f.write(resp.data)

    # write server_version (logical)
    server_ver = getattr(resp, "server_version", None)
    if server_ver is None:
        # treat as 0 if server didn't provide it
        server_ver = 0
    with open(ts_file, "w") as f:
        f.write(str(server_ver))

    os.replace(tmp_path, local_path)

    print(f"[open_or_validate] file {filename} cached locally with version={server_ver}")
    return local_path, server_ver

def write_bytes_to_local(path, data_bytes):
    filename = os.path.basename(path)
    with open(path, "wb") as f:
        f.write(data_bytes)

    print(f"Local cache for {filename} updated locally")

def write_primes_to_local(path, primes):
    filename = os.path.basename(path)
    if not os.path.exists(path):
        print("File not cached locally")
        return
    
    with open(path, "a") as f:
        for prime in primes:
            f.write(f"{prime}\n")

    print(f"Local cache for {filename} updated locally")

_stub_cache = {}

def _make_stub(hostport, timeout_connect=0.8):
    # Reuse the channel to a known leader rather than opening a new one on every upload
    ch = _stub_cache.get(hostport)
    if ch is None:
        ch = grpc.insecure_channel(hostport, options=CHANNEL_OPTIONS)
        _stub_cache[hostport] = ch
    try:
        grpc.channel_ready_future(ch).result(timeout=timeout_connect)
        return file_service_pb2_grpc.FileServiceStub(ch)
    except Exception:
        return None

def close_file(stub, path, peers_env_key="FILE_SERVICE_PEERS", max_retries=2):
    filename = os.path.basename(path)
    ts_file = path + ".ts"
    if not os.path.exists(path):
        print("No local file in cache to close")
        return

    with open(path, "rb") as f:
        data = f.read()

    print(f"Closing and updating {filename} to server")
    peers = [p for p in os.environ.get(peers_env_key, "").split(",") if p]
    deadline = time.time() + UPLOAD_RETRY_WINDOW
    while True:
        ok, hint = _upload_to(stub, filename, data, ts_file)

        # Follow leader hints first, then try every known server in case the hint is stale
        candidates = ([hint] if hint else []) + peers
        tried = set()
        while not ok and candidates:
            addr = candidates.pop(0)
            if addr in tried:
                continue
            tried.add(addr)
            addr_stub = _make_stub(addr)
            if addr_stub is None:
                continue
            ok, hint = _upload_to(addr_stub, filename, data, ts_file)
            if hint and hint not in tried:
                print(f"Redirected to leader {hint}, retrying upload there.")
                candidates.insert(0, hint)

        if ok:
            return
        if time.time() >= deadline:
            print("Failed to upload file to leader after trying hints and peers.")
            return
        print("No leader reachable (election in progress?), retrying upload in 1s")
        time.sleep(1.0)


def _upload_to(stub, filename, data, ts_file):
    """Try UploadFile on one server. Returns (True, None) once committed, else (False, leader hint or None)."""
    try:
        resp = stub.UploadFile(file_service_pb2.UploadRequest(filename=filename, data=data))
    except grpc.RpcError as e:
        print(f"Upload RPC failed: {e.code().name}")
        return False, None

    if resp.success:
        print(f"Upload result: {resp.message}")
        new_resp = stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_version=0))
        with open(ts_file, "w") as f:
            f.write(str(new_resp.server_version))
        return True, None
    if "NOT_LEADER" in resp.message:
        return False, resp.leader_host or None
    print(f"Upload rejected: {resp.message}")
    return False, None


