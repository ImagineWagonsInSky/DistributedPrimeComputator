import grpc
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
import os
import time


def validate(filename, stub, cached_ts):
    print(f"Found cached copy of '{filename}' with ts={cached_ts}, validating...")
    resp = stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=cached_ts))
    if resp.valid:
        print("Cache valid, using local copy.")
        return True
    else:
        print("Cache outdated, refetching from server.")
        return False

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