# DistributedSystems-CS

### 1. Build and start the system

From the repository root, run:

```bash
docker compose up --build
```

This command will:

* Build the shared image from the `Dockerfile`.
* Start:

  * four `file_service_*` containers,
  * one `coordinator` container,
  * one `worker` container - scalable to more.

You can view logs with:

```bash
docker compose logs -f
```

---

### 2. Scaling the workers

You can simulate multiple workers by scaling the worker service:

```bash
docker compose up --build --scale worker=5
```

This will start five independent worker containers (named `worker_1`, `worker_2`, etc.),
each connecting to the same coordinator and file service.

---

## Environment Configuration

The containers communicate via a shared Docker network.
Each component reads its configuration from environment variables (set in `docker-compose.yml`):

| Variable            | Default        | Description                            |
| ------------------- | -------------- | -------------------------------------- |
| `FILE_SERVICE_HOST` | `file_service` | Hostname of the main file service container |
| `FILE_SERVICE_PORT` | `50051`        | It's default port               |
| `COORDINATOR_HOST`  | `coordinator`  | Hostname of the coordinator container  |
| `COORDINATOR_PORT`  | `50052`        | Coordinator gRPC port                  |
| `FILE_SERVICE_PEERS` | —             | Every file server; the coordinator and workers fail over across them when one is down |
| `PEERS`             | —              | (file servers) The other file servers in the Raft cluster |
| `ADVERTISE_ADDR`    | `localhost:<RPC_PORT>` | (file servers) Address other containers reach this server on; handed to clients as the leader hint |

The file servers elect their leader with Raft (pre-vote enabled), so no server is configured as leader.

In local (non-Docker) runs, these default to `localhost` and can be overridden with `--env`.

---

## Code Overview

* `services/file_service/server.py`: gRPC file server implementation.
* `services/coordinator/coordinator.py`: Coordinator gRPC service.
* `services/coordinator/worker.py`: Worker logic connecting to coordinator and file service.
* `proto/generated/`: Generated protobuf and gRPC Python files.

All processes are started using Python’s module syntax (e.g. `python3 -m services.coordinator.coordinator`).


---

## Local Development

If you’re actively editing the source code, you can mount your local directory into the containers by adding this under each service in `docker-compose.yml`:

```yaml
volumes:
  - .:/app
```

Then code changes will be reflected without rebuilding the image.