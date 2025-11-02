import os
import subprocess

PROTO_DIR = "proto"
OUTPUT_DIR = "proto/generated"

def generate_protos():
    for proto_file in os.listdir(PROTO_DIR):
        if proto_file.endswith('.proto'):
            service_name = proto_file[:-6]
            out_dir = os.path.join(OUTPUT_DIR, service_name)
            os.makedirs(out_dir, exist_ok=True)
            
            subprocess.run([
                "python", "-m", "grpc_tools.protoc",
                f"-I{PROTO_DIR}",
                f"--python_out={out_dir}",
                f"--grpc_python_out={out_dir}",
                os.path.join(PROTO_DIR, proto_file)
            ])

if __name__ == "__main__":
    generate_protos()