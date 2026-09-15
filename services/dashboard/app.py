"""
Live dashboard for demos. Polls every service's GetStatus, turns the containers' logs into an
event feed, and lets you kill, freeze and revive containers through the Docker socket.
"""
import os
import socket
import threading
import time
from collections import deque
from concurrent import futures

import docker
import grpc
from flask import Flask, abort, jsonify, request, send_from_directory
from google.protobuf.empty_pb2 import Empty

from proto.generated.coordinator import coordinator_pb2_grpc
from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc

FILE_SERVERS = [a for a in os.environ.get("FILE_SERVICE_PEERS", "").split(",") if a]
COORDINATOR_ADDR = os.environ.get("COORDINATOR_ADDR", "coordinator:50052")
PORT = int(os.environ.get("DASHBOARD_PORT", "8080"))
POLL_INTERVAL = 0.25
# Short, so a frozen service shows as unresponsive instead of stalling the dashboard
STATUS_TIMEOUT = 0.3
# Log lines worth showing in the event feed, minus ones that fire on every upload
EVENT_MARKERS = ("[Election]", "[Leader", "[PreVote]", "[Follower]", "[failover]",
                 "JOB RESET", "SNAPSHOT", "Haven't received")
EVENT_IGNORED = ("handling UploadFile",)
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

docker_client = docker.from_env()
# Only containers from our own compose project can be seen or controlled
PROJECT = docker_client.containers.get(socket.gethostname()).labels["com.docker.compose.project"]
CONTROLLED_SERVICES = ("file_service", "coordinator", "worker")


def fields(msg):
    """Plain dict of a message's fields (MessageToDict would turn int64s into strings)."""
    return {f.name: getattr(msg, f.name) for f in msg.DESCRIPTOR.fields}


class Dashboard:
    def __init__(self):
        self.state = {"file_servers": [], "coordinator": None, "workers": []}
        self.events = deque(maxlen=500)
        self.next_event_id = 1
        self.events_lock = threading.Lock()
        self.pool = futures.ThreadPoolExecutor(max_workers=len(FILE_SERVERS))
        self.file_stubs = {addr: file_service_pb2_grpc.FileServiceStub(grpc.insecure_channel(addr))
                           for addr in FILE_SERVERS}
        self.coordinator_stub = coordinator_pb2_grpc.CoordinatorStub(grpc.insecure_channel(COORDINATOR_ADDR))
        # container id -> whether its logs are being followed / when its log stream last ended
        self.following = {}
        self.stream_ended = {}

    def containers(self):
        """name -> summary of every container we control. The sparse list is one cheap API call."""
        found = docker_client.containers.list(
            all=True, sparse=True, filters={"label": f"com.docker.compose.project={PROJECT}"})
        result = {}
        for c in found:
            service = c.attrs["Labels"].get("com.docker.compose.service", "")
            if service.startswith(CONTROLLED_SERVICES):
                name = c.attrs["Names"][0].lstrip("/")
                result[name] = {"id": c.id[:12], "service": service, "state": c.attrs["State"],
                                "label": name.removeprefix(f"{PROJECT}-")}
        return result

    # --- live state ---

    def poll_forever(self):
        while True:
            try:
                self.state = self._snapshot()
            except Exception as e:
                print(f"[dashboard] poll failed: {e}")
            time.sleep(POLL_INTERVAL)

    def _snapshot(self):
        containers = self.containers()
        server_calls = {addr: self.pool.submit(self._server_status, addr) for addr in FILE_SERVERS}
        coordinator = self._coordinator_status()

        file_servers = []
        for addr, call in server_calls.items():
            name = addr.split(":")[0]
            file_servers.append({"name": name, "state": containers.get(name, {}).get("state", "missing"),
                                 "status": call.result()})

        # Worker ids start with the container's hostname, which is its short container id
        worker_status = {w["worker_id"].split("-")[0]: w for w in (coordinator or {}).get("workers", [])}
        workers = [{"name": name, "label": c["label"], "state": c["state"], "status": worker_status.get(c["id"])}
                   for name, c in sorted(containers.items()) if c["service"] == "worker"]

        return {
            "file_servers": file_servers,
            "coordinator": {"name": "coordinator",
                            "state": containers.get("coordinator", {}).get("state", "missing"),
                            "status": coordinator},
            "workers": workers,
        }

    def _server_status(self, addr):
        try:
            s = self.file_stubs[addr].GetStatus(file_service_pb2.StatusRequest(), timeout=STATUS_TIMEOUT)
        except grpc.RpcError:
            return None
        status = fields(s)
        status["unreachable_peers"] = list(s.unreachable_peers)
        return status

    def _coordinator_status(self):
        try:
            c = self.coordinator_stub.GetStatus(Empty(), timeout=STATUS_TIMEOUT)
        except grpc.RpcError:
            return None
        status = fields(c)
        status["workers"] = [fields(w) for w in c.workers]
        return status

    # --- event feed ---

    def add_event(self, source, text):
        with self.events_lock:
            self.events.append({"id": self.next_event_id, "time": time.time(), "source": source, "text": text})
            self.next_event_id += 1

    def events_after(self, after):
        with self.events_lock:
            return [e for e in self.events if e["id"] > after]

    def watch_logs_forever(self):
        """Follow the logs of every running container, re-attaching whenever one is restarted."""
        started = int(time.time())
        while True:
            try:
                for c in self.containers().values():
                    if c["state"] != "running" or self.following.get(c["id"]):
                        continue
                    self.following[c["id"]] = True
                    since = self.stream_ended.get(c["id"], started)
                    threading.Thread(target=self._follow, args=(c, since), daemon=True).start()
            except Exception as e:
                print(f"[dashboard] log watcher failed: {e}")
            time.sleep(1.0)

    def _follow(self, c, since):
        try:
            stream = docker_client.containers.get(c["id"]).logs(stream=True, follow=True, since=since)
            for chunk in stream:
                for line in chunk.decode(errors="replace").splitlines():
                    if any(m in line for m in EVENT_MARKERS) and not any(m in line for m in EVENT_IGNORED):
                        self.add_event(c["label"], line.strip())
        except Exception as e:
            print(f"[dashboard] log stream for {c['label']} ended: {e}")
        finally:
            # the stream ends when the container stops; pick up from here once it's back
            self.stream_ended[c["id"]] = int(time.time())
            self.following[c["id"]] = False

    # --- controls ---

    def control(self, name, action):
        c = self.containers().get(name)
        if c is None:
            abort(404)
        container = docker_client.containers.get(c["id"])
        if action == "kill":
            # SIGKILL: an abrupt crash. Docker won't kill a paused container, so thaw it first.
            if c["state"] == "paused":
                container.unpause()
            container.kill()
        elif action == "freeze":
            # still "up" but unresponsive, like a hung process
            container.pause()
        elif action == "revive":
            container.unpause() if c["state"] == "paused" else container.start()
        else:
            abort(400)
        self.add_event("you", f"{action} {c['label']}")

    def reset_job(self):
        self.coordinator_stub.ResetJob(Empty(), timeout=30)
        # workers exit once the job runs out, so bring them back for the new one
        for c in self.containers().values():
            if c["service"] == "worker" and c["state"] == "exited":
                docker_client.containers.get(c["id"]).start()
        self.add_event("you", "started a new job")


app = Flask(__name__, static_folder=None)
dashboard = Dashboard()


@app.get("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.get("/api/state")
def state():
    return jsonify(dashboard.state)


@app.get("/api/events")
def events():
    return jsonify(dashboard.events_after(request.args.get("after", 0, type=int)))


@app.post("/api/containers/<name>/<action>")
def control(name, action):
    try:
        dashboard.control(name, action)
    except docker.errors.APIError as e:
        return jsonify(error=e.explanation), 409
    return jsonify(ok=True)


@app.post("/api/job/reset")
def reset_job():
    try:
        dashboard.reset_job()
    except grpc.RpcError as e:
        return jsonify(error=f"coordinator unavailable ({e.code().name})"), 503
    return jsonify(ok=True)


if __name__ == "__main__":
    threading.Thread(target=dashboard.poll_forever, daemon=True).start()
    threading.Thread(target=dashboard.watch_logs_forever, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
