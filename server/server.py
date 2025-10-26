import grpc
from concurrent import futures
import file_service_pb2, file_service_pb2_grpc
import os

DATA_DIR = "server_store"
os.makedirs(DATA_DIR, exist_ok=True)

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
        return file_service_pb2.OpenResponse(success=True, data=data, message="File sent")

    # TODO: be made with TestAuth checksum stuff
    def UploadFile(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        with open(path, "wb") as f:
            f.write(request.data)
        return file_service_pb2.UploadResponse(success=True, message="File uploaded succesfully")
    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    file_service_pb2_grpc.add_FileServiceServicer_to_server(FileServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    print("Server listening on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
