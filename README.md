# DistributedSystems-CS

## Setup & Usage

Assuming linux usage.

1. Create and activate virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install requirements:
```bash
pip3 install -r requirements.txt
```
3. Generate gRPC code (run in both client/ and server/ directories):
```bash
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. file_service.proto
```

4. Run server (in one terminal):
```bash
cd server/
python3 server.py
```

5. Run client (in another terminal):
```bash
cd client/
python3 client.py
```