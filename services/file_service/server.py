import math

import grpc
from concurrent import futures
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
import os
import time

DATA_DIR = "services/file_service/server_store"
SUBDIVISIONS_DIR = "services/file_service/server_store/subdivisions"
os.makedirs(DATA_DIR, exist_ok=True)


def file_timestamp(path):
    return int(os.path.getmtime(path))

class FileServiceServicer(file_service_pb2_grpc.FileServiceServicer):
    def __init__(self):
        self._cleanup_temp_files()  # Cleanup temp files on startup

    def _cleanup_temp_files(self):
        """Remove any .tmp files left from crashed uploads"""
        for filename in os.listdir(DATA_DIR):
            if filename.endswith('.tmp'):
                tmp_path = os.path.join(DATA_DIR, filename)
                try:
                    os.remove(tmp_path)
                    print(f"[startup] Cleaned up temp file: {filename}")
                except OSError:
                    pass


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
    '''
    def UploadFile(self, request, context):
        path = os.path.join(DATA_DIR, request.filename)
        with open(path, "wb") as f:
            f.write(request.data)
        return file_service_pb2.UploadResponse(success=True, message="File uploaded successfully")
    '''

    def UploadFile(self, request, context):
      filename = request.filename
      data = request.data

      # ATOMIC WRITE PATTERN
      # Write to temp file first, then atomically rename to avoid partial files
      file_path = os.path.join(DATA_DIR, filename)
      tmp_path = file_path + ".tmp"

      try:
          # Write to temp file first
          with open(tmp_path, "wb") as f:
              f.write(data)

          # Sync to disk (ensures data is persisted before rename)
          with open(tmp_path, "rb") as f:
              os.fsync(f.fileno())

          # Atomic rename (either succeeds completely or not at all)
          os.replace(tmp_path, file_path)

          print(f"[UploadFile] Successfully uploaded {filename}")
          return file_service_pb2.UploadResponse(
              success=True,
              message=f"File {filename} uploaded successfully"
          )

      except Exception as e:
          # Clean up temp file on error
          if os.path.exists(tmp_path):
              try:
                  os.remove(tmp_path)
              except OSError:
                  pass

          print(f"[UploadFile] Error: {e}")
          return file_service_pb2.UploadResponse(
              success=False,
              message=f"Upload failed: {str(e)}"
          )

    
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

    def RequestSubdivisions(self, request, context):
        #Create a directory for holding subdivisions, separately from other files
        os.makedirs(SUBDIVISIONS_DIR, exist_ok=True)

        size = request.subdivision_size
        all_files = os.listdir(DATA_DIR)
        for f in all_files:
            path = os.path.join(DATA_DIR, f)
            if os.path.isfile(f):
                self.divide_file(f, size)

        return file_service_pb2.RequestDivisionResponse(success=True)


#Proposition for file division function, a bit ugly, could be cleaned up.
    def divide_file(self, filename, subdivision_size):
        with open(os.path.join(DATA_DIR, filename), "rb") as f:
            main_file_lines = f.readlines()
            subdivision_count = math.ceil(len(main_file_lines) / subdivision_size)
            for i in range(subdivision_count):
                #sd{i} meaning subdivision of number 'i' e.g. inputfile_003_sd2.txt
                current_subdivision_filename = f"{filename.strip(".txt")}_sd{i}.txt"

                sd_file = open(os.path.join(DATA_DIR, current_subdivision_filename), "wb")
                try:
                    sd_file.write(main_file_lines[i * subdivision_size :
                                                  (i + 1) * subdivision_size])
                except IndexError:
                    try:
                        leftover_lines = len(main_file_lines) % subdivision_size
                        sd_file.write(main_file_lines[i * subdivision_size :
                                                      i * subdivision_size + leftover_lines])
                    except:
                        sd_file.close()
                        os.remove(sd_file.name)
                        print("Failed to divide file")


                sd_file.close()

        print(f"Created {subdivision_count} subdivisions for file {filename}")

    #Clear up the subdivion files for particular original file, might as well clear them all at the end, but that is an easy change to make
    def cleanup_subdivisions(self, filename):
        for f in os.listdir(SUBDIVISIONS_DIR):
            #This part feels kinda ugly, there should be a better way to do this
            if f"{filename.strip(".txt")}" in f:
                try:
                    sd_file = os.path.join(DATA_DIR, f)
                    os.remove(sd_file)
                except OSError:
                    pass
        print(f"Cleaned up subdivisions for {filename}")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=[('grpc.max_send_message_length', -1),('grpc.max_receive_message_length', -1),])
    file_service_pb2_grpc.add_FileServiceServicer_to_server(FileServiceServicer(), server)
    server.add_insecure_port("0.0.0.0:50051")
    print("Server listening with TestAuth support on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
