import grpc
import file_service_pb2, file_service_pb2_grpc
import os
import time

import argparse

# Can add a different cache directory to test cache validation on independent clients
parser = argparse.ArgumentParser()
parser.add_argument("--cache-dir", default="client_cache")
args = parser.parse_args()

CACHE_DIR = args.cache_dir
os.makedirs(CACHE_DIR, exist_ok=True)


def validate(filename, stub, cached_ts):
    print(f"Found cached copy of '{filename}' with ts={cached_ts}, validating...")
    resp = stub.TestAuth(file_service_pb2.TestAuthRequest(filename=filename, client_timestamp=cached_ts))
    if resp.valid:
        print("Cache valid, using local copy.")
        return True
    else:
        print("Cache outdated, refetching from server.")
        return False

def open_or_validate(stub, filename):
    """
    If cached copy exists, validate with server before reuse.
    """
    local_path = os.path.join(CACHE_DIR, filename)
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

def write_local(filename, new_data):
    """
    Simulating just a local write
    """
    path = os.path.join(CACHE_DIR, filename)
    if not os.path.exists(path):
        print("File not cached locally")
        return
    with open(path, "wb") as f:
        f.write(new_data)
    print(f"Local cache for {filename} updated locally")

def close_file(stub, filename):
    """
    Upload cached file and update timestamp.
    """
    path = os.path.join(CACHE_DIR, filename)
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

def main():
    channel = grpc.insecure_channel("localhost:50051")
    stub = file_service_pb2_grpc.FileServiceStub(channel)
    fname = "demo.txt"

    local_file, ts = open_or_validate(stub, fname)
    with open(local_file, "rb") as f:
        print("Local read:", f.read().decode())

    write_local(fname, b"new data1")
    close_file(stub, fname)

if __name__ == "__main__":
    main()
