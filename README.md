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
You also have the option of making a different cache for each "client" to see cache validation in action:
```
python3 client.py --cache-dir clientA_cache
python3 client.py --cache-dir clientB_cache

```