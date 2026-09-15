# Dockerfile
FROM python:3.12.3

# set noninteractive
ENV PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy full repo
COPY . .

# Generate protobuf files
RUN python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    proto/coordinator.proto && \
    python -m grpc_tools.protoc \
    -I. \
    --python_out=. \
    --grpc_python_out=. \
    proto/file_service.proto && \
    # Move generated files to the correct location
    mv proto/coordinator_pb2.py proto/generated/coordinator/ && \
    mv proto/coordinator_pb2_grpc.py proto/generated/coordinator/ && \
    mv proto/file_service_pb2.py proto/generated/file_service/ && \
    mv proto/file_service_pb2_grpc.py proto/generated/file_service/ && \
    # Fix imports in generated _grpc.py files
    sed -i 's/from proto import coordinator_pb2/from proto.generated.coordinator import coordinator_pb2/g' proto/generated/coordinator/coordinator_pb2_grpc.py && \
    sed -i 's/from proto import file_service_pb2/from proto.generated.file_service import file_service_pb2/g' proto/generated/file_service/file_service_pb2_grpc.py

#
