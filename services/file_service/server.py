import grpc
from concurrent import futures
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
import os
import time

DATA_DIR = "services/file_service/server_store"
os.makedirs(DATA_DIR, exist_ok=True)

def file_timestamp(path):
    return int(os.path.getmtime(path))

class FileServiceServicer(file_service_pb2_grpc.FileServiceServicer):
    def CreateFile(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        if os.path.exists(path):
            return file_service_pb2.CreateResponse(success=False, message="File already exists")
        with open(path, "wb") as f:
            f.write(request.data)
        return file_service_pb2.CreateResponse(success=True, message="File created")

    def OpenFile(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        if not os.path.exists(path):
            return file_service_pb2.OpenResponse(success=False, message="No such file")
        with open(path, "rb") as f:
            data = f.read()
        ts = file_timestamp(path)
        return file_service_pb2.OpenResponse(success=True, data=data, message="File sent", server_timestamp=ts)

    def UploadFile(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        with open(path, "wb") as f:
            f.write(request.data)
        return file_service_pb2.UploadResponse(success=True, message="File uploaded succesfully")
    
    def TestAuth(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        if not os.path.exists(path):
            return file_service_pb2.TestAuthResponse(valid=False, message="File not found", server_timestamp=0)
        server_ts = file_timestamp(path)
        if server_ts == request.client_timestamp:
            return file_service_pb2.TestAuthResponse(valid=True, message="Cache Valid", server_timestamp=server_ts)
        else: 
            return file_service_pb2.TestAuthResponse(valid=False, message="Cache outdated", server_timestamp=server_ts)
    
    def ListFiles(self, request, context):
        """
        Returns repeated list of tuples:
        (filename: string, size: uint64)
        Optional offset and file limit
        """
        files = []
        all_files = os.listdir(DATA_DIR)
        start = request.offset
        end = start + request.limit if request.limit > 0 else len(all_files)

        for name in all_files[start:end]:
            path = os.path.join(DATA_DIR, name)
            if os.path.isfile(path):
                files.append(file_service_pb2.FileTuple(
                    filename=name,
                    size=os.path.getsize(path)
                ))

        return file_service_pb2.ListFilesResponse(files=files)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    file_service_pb2_grpc.add_FileServiceServicer_to_server(FileServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    print("Server listening with TestAuth support on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
