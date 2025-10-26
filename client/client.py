import grpc
import file_service_pb2, file_service_pb2_grpc
import os

CACHE_DIR = "client_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def open_file(stub, filename):
    """
    Fetch whole file and cache it locally
    """
    resp = stub.OpenFile(file_service_pb2.OpenRequest(filename=filename))
    if not resp.success:
        print("Open failed:", resp.message)
        return None
    local_path = os.path.join(CACHE_DIR, filename)
    with open(local_path, "wb") as f:
        f.write(resp.data)
    print(f"File {filename} saved locally in cache")
    return local_path

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
    Upload closed file back to server
    """
    path = os.path.join(CACHE_DIR, filename)
    if not os.path.exists(path):
        print("No local file in cache to close")
        return
    with open(path, "rb") as f:
        data = f.read()
    resp = stub.UploadFile(file_service_pb2.UploadRequest(filename=filename, data=data))
    print(f"Upload result: {resp.message}")    

def main():
    channel = grpc.insecure_channel("localhost:50051")
    stub = file_service_pb2_grpc.FileServiceStub(channel)
    fname = "demo.txt"

    local_file = open_file(stub, fname)

    write_local(fname, b"new data")
    close_file(stub, fname)

if __name__ == "__main__":
    main()
