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

### 3. Live demo dashboard

To show the system to other people, start it with the demo settings:

```bash
docker compose -f docker-compose.yml -f docker-compose.demo.yml up --build
```

Then open http://localhost:8080. The demo settings make the job last a couple of minutes
(small tasks, a 2 second delay per task, three workers) so there is time to break things.

The page shows the Raft cluster (leader, followers, each follower's election countdown, heartbeats
travelling from the leader), the coordinator's progress, each worker's current task, and a live
feed of elections, failovers and syncs taken from the containers' logs.

Every service has three buttons:

* **Kill**: SIGKILL the container, like a crash.
* **Freeze**: pause it. It stays up but stops responding, like a hung process.
* **Revive**: start or unpause it again.

**New job** queues the whole job again and restarts the workers.

Things to try:

1. **Kill the leader.** The followers' countdowns run out, one wins a new election, and the
   coordinator and workers switch to a live file server.
2. **Freeze a follower.** The leader can't reach it, but nobody starts an election.
3. **Kill a follower while the job runs, then revive it.** It comes back behind the leader and
   is synced up to date.
4. **Kill the coordinator, then revive it.** Workers wait and retry; the coordinator restores its
   last snapshot and carries on.
5. **Kill a worker.** After 15 seconds without heartbeats the coordinator hands its task to
   another worker.

The dashboard mounts the Docker socket so it can control containers. That is root-equivalent
access to the host, so it only listens on localhost.

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
Every committed upload gets the next index in a cluster-wide sequence. A server that missed uploads
(e.g. it was down) is synced by the leader on its next heartbeat, refuses reads until then, and cannot
win an election, since servers only vote for candidates holding every upload they hold.

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