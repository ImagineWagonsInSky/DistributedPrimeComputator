import grpc
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
import os
import time

#test_code 1
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
            # stop if we used all retries or this is not a transient error
            if attempt >= retries or code not in retry_codes:
                print(f"[rpc_retry] RPC failed, no more retries")
                raise
            # exponential backoff: 1s, 2s, 4s, ...
            sleep_for = backoff * (2 ** (attempt - 1))
            print(f"[rpc_retry] RPC {code}, retrying in {sleep_for:.1f}s (attempt {attempt}/{retries})...")
            time.sleep(sleep_for)
#test_end

'''
def validate(filename, stub, cached_ts):
    print(f"Found cached copy of '{filename}' with ts={cached_ts}, validating...")
    resp = stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=cached_ts))
    if resp.valid:
        print("Cache valid, using local copy.")
        return True
    else:
        print("Cache outdated, refetching from server.")
        return False
'''
    
#test_code 4
def validate(filename, stub, cached_ts):
    print(f"Found cached copy of '{filename}' with ts={cached_ts}, validating...")
    try:
        resp = _rpc_retry(
            stub.TestAuth,
            file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=cached_ts),
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
#test_end

'''
def open_or_validate(stub, local_path):
    """
    If cached copy exists, validate with server before reuse.
    """
    filename = os.path.basename(local_path)
    ts_file = local_path + ".ts"

    if os.path.exists(local_path) and os.path.exists(ts_file):
        with open(ts_file) as f:
            cached_ts = int(f.read().strip())
        if validate(filename=filename, stub=stub, cached_ts=cached_ts):
            return local_path, cached_ts
    else:
        print("No cached copy, fetching from server.")

    # Fetch new copy
    resp = stub.OpenFile(file_service_pb2.OpenRequest(filename=filename))
    if not resp.success:
        print("Open failed:", resp.message)
        return None, None
    with open(local_path, "wb") as f:
        f.write(resp.data)
    with open(ts_file, "w") as f:
        f.write(str(resp.server_timestamp))
    print(f"File {filename} fetched and saved locally in cache with ts={resp.server_timestamp}.")
    return local_path, resp.server_timestamp
'''

#test_code 2
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

    # 1) try to reuse local cache
    if os.path.exists(local_path) and os.path.exists(ts_file):
        with open(ts_file) as f:
            cached_ts = int(f.read().strip())
        # this already logs "Cache valid" or "Cache outdated"
        if validate(filename=filename, stub=stub, cached_ts=cached_ts):
            return local_path, cached_ts
        # if not valid, just fall through to re-download
    else:
        print(f"No cached copy for {filename}, fetching from server...")

    # 2) need to fetch from server → wrap RPC in retry
    tmp_path = local_path + ".tmp"

    try:
        # Timer for pure gRPC call
        grpc_start_time = time.time()
        print(f"[TIMER] Pure gRPC OpenFile call STARTED for '{filename}'")

        # === MANUAL TEST PAUSE FOR SERVER CRASH ===
        # Test server crash during READ operation
        #print(f"\n[TEST MODE] Pausing for 15 seconds... Kill FILE-SERVER NOW to test read retry!")
        #for i in range(15, 0, -1):
        #    print(f"[TEST MODE] OpenFile RPC starts in {i} seconds... (docker kill file-server)")
        #    time.sleep(1)
        #print(f"[TEST MODE] Starting OpenFile RPC NOW - Kill file-server to test retry!")
        # === END TEST PAUSE ===

        resp = _rpc_retry(
            stub.OpenFile,
            file_service_pb2.OpenRequest(filename=filename),
            retries=max_retries,
            backoff=1.0,
        )

        grpc_end_time = time.time()
        grpc_duration = grpc_end_time - grpc_start_time
        print(f"[TIMER] Pure gRPC OpenFile call ENDED - Duration: {grpc_duration:.3f}s (includes retries if any)")

    except grpc.RpcError:
        print(f"[open_or_validate] OpenFile RPC failed after retries; will retry later.")
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        return None, None

    if not resp.success:
        print(f"[open_or_validate] OpenFile failed from server: {resp.message}")
        return None, None

    # 3) write to TEMP first so partial downloads never become the real cache
    with open(tmp_path, "wb") as f:
        f.write(resp.data)

    # 4) write timestamp
    with open(ts_file, "w") as f:
        f.write(str(resp.server_timestamp))

    # 5) atomically promote temp -> real
    os.replace(tmp_path, local_path)

    print(f"[open_or_validate] file {filename} cached locally with ts={resp.server_timestamp}")
    return local_path, resp.server_timestamp
#test_end

def write_bytes_to_local(path, data_bytes):
    """
    Simulating a local write, writes raw bytes to the local cache in with
    overwrite mode, creating a file if it doesn't exist.
    """
    filename = os.path.basename(path)
    with open(path, "wb") as f:
        f.write(data_bytes)

    print(f"Local cache for {filename} updated locally")

def write_primes_to_local(path, primes):
    """
    A local write from list of primes
    """
    filename = os.path.basename(path)
    if not os.path.exists(path):
        print("File not cached locally")
        return
    
    with open(path, "a") as f:
        for prime in primes:
            f.write(f"{prime}\n")

    print(f"Local cache for {filename} updated locally")


def close_file(stub, path):
    """
    Upload cached file and update timestamp.
    """
    filename = os.path.basename(path)
    ts_file = path + ".ts"
    if not os.path.exists(path):
        print("No local file in cache to close")
        return
    with open(path, "rb") as f:
        data = f.read()
    resp = stub.UploadFile(file_service_pb2.UploadRequest(filename=filename, data=data))
    print(f"Upload result: {resp.message}")
    # Fetch new server timestamp after upload
    new_resp = stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=0))
    with open(ts_file, "w") as f:
        f.write(str(new_resp.server_timestamp))
'''

def close_file(stub, path):
    """
    Upload cached file and update timestamp.
    Server-side handles atomic writes via temp files.
    """
    filename = os.path.basename(path)
    ts_file = path + ".ts"
    if not os.path.exists(path):
        print("No local file in cache to close")
        return

    with open(path, "rb") as f:
        data = f.read()

    # === MANUAL TEST PAUSE FOR WRITE ===
    # Uncomment to test server crash during WRITE operation
    # print(f"\n[TEST MODE] Pausing for 15 seconds... Kill FILE-SERVER NOW!")
    # for i in range(15, 0, -1):
    #     print(f"[TEST MODE] Upload starts in {i} seconds... (docker kill file-server)")
    #     time.sleep(1)
    # print("[TEST MODE] Starting upload NOW!")
    # === END TEST PAUSE ===

    resp = stub.UploadFile(file_service_pb2.UploadRequest(filename=filename, data=data))
    print(f"Upload result: {resp.message}")

    # Fetch new server timestamp after upload
    new_resp = stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=0))
    with open(ts_file, "w") as f:
        f.write(str(new_resp.server_timestamp))
'''