import grpc
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
import os
import time

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
            sleep_for = backoff * (2 ** (attempt - 1))
            print(f"[rpc_retry] RPC {code}, retrying in {sleep_for:.1f}s (attempt {attempt}/{retries})...")
            time.sleep(sleep_for)
    
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
        #grpc_start_time = time.time()
        #print(f"[TIMER] Pure gRPC OpenFile call STARTED for '{filename}'")

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

        #grpc_end_time = time.time()
        #grpc_duration = grpc_end_time - grpc_start_time
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

    with open(ts_file, "w") as f:
        f.write(str(resp.server_timestamp))

    os.replace(tmp_path, local_path)

    print(f"[open_or_validate] file {filename} cached locally with ts={resp.server_timestamp}")
    return local_path, resp.server_timestamp

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

def _make_stub(hostport, timeout_connect=0.8):
    ch = grpc.insecure_channel(hostport, options=[
        ("grpc.max_send_message_length", -1),
        ("grpc.max_receive_message_length", -1),
    ])
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

    try:
        resp = stub.UploadFile(file_service_pb2.UploadRequest(filename=filename, data=data))
    except grpc.RpcError as e:
        print(f"Upload RPC error on initial stub: {e}. Will attempt discovery.")
        resp = None

    if resp and getattr(resp, "success", False):
        print(f"Upload result: {resp.message}")
        new_resp = stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=0))
        with open(ts_file, "w") as f:
            f.write(str(new_resp.server_timestamp))
        return

    leader_hint = None
    if resp:
        if getattr(resp, "message", "") and "NOT_LEADER" in str(resp.message):
            leader_hint = getattr(resp, "leader_host", None)

    if leader_hint:
        print(f"Redirected to leader {leader_hint}, retrying upload there.")
        leader_stub = _make_stub(leader_hint)
        if leader_stub:
            try:
                resp2 = leader_stub.UploadFile(file_service_pb2.UploadRequest(filename=filename, data=data))
                if getattr(resp2, "success", False):
                    # print(f"Upload result (leader): {resp2.message}")
                    new_resp = leader_stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=0))
                    with open(ts_file, "w") as f:
                        f.write(str(new_resp.server_timestamp))
                    return
            except grpc.RpcError as e:
                print(f"Upload RPC to hinted leader failed: {e}")

    peers_str = os.environ.get(peers_env_key, "")
    peers = [p for p in peers_str.split(",") if p]
    tried = set()
    for peer in peers:
        if peer in tried:
            continue
        tried.add(peer)
        # print(f"Trying peer {peer} as potential leader...")
        peer_stub = _make_stub(peer)
        if not peer_stub:
            continue
        try:
            resp3 = peer_stub.UploadFile(file_service_pb2.UploadRequest(filename=filename, data=data))
        except grpc.RpcError as e:
            # print(f"Upload RPC to peer {peer} failed: {e}")
            continue

        if getattr(resp3, "success", False):
            # print(f"Upload succeeded on peer {peer} (leader).")
            new_resp = peer_stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=0))
            with open(ts_file, "w") as f:
                f.write(str(new_resp.server_timestamp))
            return
        else:
            if getattr(resp3, "message", "") and "NOT_LEADER" in str(resp3.message):
                hint = getattr(resp3, "leader_host", None)
                if hint and hint not in tried:
                    # print(f"Peer {peer} redirected us to {hint}, trying that next.")
                    tried.add(hint)
                    hint_stub = _make_stub(hint)
                    if hint_stub:
                        try:
                            resp4 = hint_stub.UploadFile(file_service_pb2.UploadRequest(filename=filename, data=data))
                            if getattr(resp4, "success", False):
                                # print(f"Upload succeeded on hinted leader {hint}.")
                                new_resp = hint_stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=0))
                                with open(ts_file, "w") as f:
                                    f.write(str(new_resp.server_timestamp))
                                return
                        except grpc.RpcError as e:
                            print(f"Upload RPC to hinted leader {hint} failed: {e}")
    print("Failed to upload file to leader after trying hints and peers.")


# def close_file(stub, path):
#     filename = os.path.basename(path)
#     ts_file = path + ".ts"
#     if not os.path.exists(path):
#         print("No local file in cache to close")
#         return
#     with open(path, "rb") as f:
#         data = f.read()
#     resp = stub.UploadFile(file_service_pb2.UploadRequest(filename=filename, data=data))
#     print(f"Upload result: {resp.message}")
#     new_resp = stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=0))
#     with open(ts_file, "w") as f:
#         f.write(str(new_resp.server_timestamp))

