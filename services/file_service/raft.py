import grpc
from concurrent import futures
import os
import threading
import time
import random

from proto.generated.file_service import file_service_pb2
from .utils import DATA_DIR

# Election and heartbeat timing. All timers use time.monotonic(): a wall-clock jump
# (NTP correction, host suspend) must not trigger or suppress elections.
ELECTION_TIMEOUT = (3.0, 7.0)
HEARTBEAT_INTERVAL = 1.0
# Per-RPC deadlines. These must stay well below the election timeout so a dead or hung
# peer can never delay the leader's heartbeats to the healthy ones.
HEARTBEAT_RPC_TIMEOUT = 0.5
VOTE_RPC_TIMEOUT = 1.0
TIMER_TICK = 0.05
SYNC_RPC_TIMEOUT = 10.0


class RaftManager:
    def __init__(self, serv):
        # serv is the FileServiceServicer instance; its lock guards all raft state
        self.serv = serv
        self.pool = futures.ThreadPoolExecutor(max_workers=max(4, 2 * len(serv.peers)))
        self.election_deadline = time.monotonic() + random.uniform(*ELECTION_TIMEOUT)
        self.last_leader_contact = float("-inf")
        self._wake_heartbeats = threading.Event()
        # peers with a heartbeat RPC still outstanding, so a slow peer never piles up calls
        self._inflight = set()
        self._inflight_lock = threading.Lock()
        # peers whose last heartbeat failed, only used to avoid logging every second
        self._unreachable = set()
        # followers currently being brought up to date
        self._syncing = set()

    def start(self):
        with self.serv.lock:
            self.reset_election_timer()
        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

    def reset_election_timer(self):
        """Push the election deadline out by a fresh random timeout. Caller should hold serv.lock."""
        self.election_deadline = time.monotonic() + random.uniform(*ELECTION_TIMEOUT)

    def heard_from_leader(self):
        """Record a valid heartbeat from the current leader. Caller should hold serv.lock."""
        self.last_leader_contact = time.monotonic()
        self.reset_election_timer()

    def leader_recently_seen(self):
        """True if this node is the leader or heard from one within the minimum election timeout.
        Caller should hold serv.lock."""
        return (self.serv.role == "leader"
                or time.monotonic() - self.last_leader_contact < ELECTION_TIMEOUT[0])

    def election_timer(self):
        while True:
            time.sleep(TIMER_TICK)
            with self.serv.lock:
                if self.serv.role == "leader":
                    self.reset_election_timer()
                    continue
                if time.monotonic() < self.election_deadline:
                    continue
                # if this round fails or splits, the timer retries after a new random timeout
                self.reset_election_timer()
            self.hold_election()

    def hold_election(self):
        majority = (self.serv.cluster_size // 2) + 1

        # Pre-vote: check a majority would vote for us before bumping our term. Peers refuse
        # while they still hear from a leader, so a node that only lost contact itself (e.g. it
        # just rejoined) can't force the cluster into a new term and depose a healthy leader.
        with self.serv.lock:
            proposed_term = self.serv.current_term + 1
            asked_at = time.monotonic()
        pre_votes = self._gather_votes(proposed_term, pre_vote=True)
        if pre_votes < majority:
            print(f"[PreVote] {self.serv.server_id} has no majority for term {proposed_term} "
                  f"(votes={pre_votes}), staying follower")
            return

        with self.serv.lock:
            # a leader may have appeared or our term moved while we were asking
            if (self.serv.role == "leader" or self.serv.current_term + 1 != proposed_term
                    or self.last_leader_contact > asked_at):
                return
            self.serv.current_term = proposed_term
            self.serv.role = "candidate"
            self.serv.voted_for = self.serv.server_id
            self.serv.persist()
        print(f"[Election] {self.serv.server_id} starting election for term {proposed_term}")

        votes = self._gather_votes(proposed_term, pre_vote=False)

        with self.serv.lock:
            if self.serv.current_term != proposed_term or self.serv.role != "candidate":
                return
            if votes >= majority:
                self.serv.role = "leader"
                self.serv.leader_host = self.serv.advertise_addr
                # We may hold an upload the old leader committed without telling us. We won
                # because no majority has anything newer, so commit it ourselves.
                if self.serv.pending:
                    print(f"[Leader] {self.serv.server_id} committing pending upload at index {self.serv.pending['index']}")
                    self.serv.apply_pending()
                self.serv.persist()
                print(f"[Leader] {self.serv.server_id} WON elected leader (term {proposed_term}, votes={votes})")
                # assert leadership immediately instead of waiting for the next interval
                self._wake_heartbeats.set()
            else:
                print(f"[Election] {self.serv.server_id} lost election for term {proposed_term} (votes={votes})")
                self.serv.role = "follower"

    def _gather_votes(self, term, pre_vote):
        """Ask every peer for its vote in term. Returns the vote count including our own, stopping
        as soon as there is a majority rather than waiting on unreachable peers. Returns 0 if the
        round was overtaken by a newer term or leader."""
        votes = 1
        majority = (self.serv.cluster_size // 2) + 1
        with self.serv.lock:
            last_term, last_index = self.serv.last_entry()
        req = file_service_pb2.VoteRequest(term=term, candidate_id=self.serv.server_id, pre_vote=pre_vote,
                                           last_term=last_term, last_index=last_index)
        pending = [self.pool.submit(self._request_vote, peer, stub, req)
                   for peer, stub in self.serv.peer_stubs.items()]

        try:
            for fut in futures.as_completed(pending, timeout=VOTE_RPC_TIMEOUT + 0.5):
                if votes >= majority:
                    break
                peer, resp = fut.result()
                if resp is None:
                    continue
                with self.serv.lock:
                    if resp.term > self.serv.current_term:
                        print(f"[Election] peer {peer} has higher term {resp.term}, adopting it as follower")
                        self.serv.step_down(resp.term)
                        return 0
                    if not pre_vote and (self.serv.role != "candidate" or self.serv.current_term != term):
                        # another leader's heartbeat or a newer election overtook us
                        return 0
                if resp.vote_granted:
                    votes += 1
                    if not pre_vote:
                        print(f"[Election] received vote from {peer} (term={resp.term})")
                elif not pre_vote:
                    print(f"[Election] vote denied by {peer} (term={resp.term})")
        except futures.TimeoutError:
            pass
        return votes

    def _request_vote(self, peer, stub, req):
        try:
            return peer, stub.RequestVote(req, timeout=VOTE_RPC_TIMEOUT)
        except grpc.RpcError as e:
            if not req.pre_vote:
                print(f"[Election] no response from {peer}: {e.code().name}")
            return peer, None

    def heartbeat_loop(self):
        while True:
            self._wake_heartbeats.wait(HEARTBEAT_INTERVAL)
            self._wake_heartbeats.clear()
            with self.serv.lock:
                if self.serv.role != "leader":
                    continue
                term = self.serv.current_term
                commit = (self.serv.commit_term, self.serv.commit_index)
                req = file_service_pb2.HeartbeatRequest(
                    leader_id=self.serv.advertise_addr, term=term,
                    leader_commit_term=commit[0], leader_commit_index=commit[1])

            # Fire all heartbeats concurrently without waiting on any of them
            for peer, stub in self.serv.peer_stubs.items():
                with self._inflight_lock:
                    if peer in self._inflight:
                        continue
                    self._inflight.add(peer)
                fut = stub.Heartbeat.future(req, timeout=HEARTBEAT_RPC_TIMEOUT)
                fut.add_done_callback(lambda f, p=peer, t=term, c=commit: self._on_heartbeat_reply(p, t, c, f))

    def _on_heartbeat_reply(self, peer, term, commit, fut):
        with self._inflight_lock:
            self._inflight.discard(peer)
        try:
            resp = fut.result()
        except grpc.RpcError as e:
            if peer not in self._unreachable:
                self._unreachable.add(peer)
                print(f"[Leader:{self.serv.server_id}] heartbeat to {peer} failed: {e.code().name}")
            return

        if peer in self._unreachable:
            self._unreachable.discard(peer)
            print(f"[Leader:{self.serv.server_id}] {peer} is reachable again")

        # The heartbeat carried our commit point, so a follower reporting anything else is missing uploads
        if resp.ok and (resp.commit_term, resp.commit_index) != commit:
            self._start_sync(peer)

        if not resp.ok and resp.term > term:
            with self.serv.lock:
                if resp.term > self.serv.current_term:
                    print(f"[Leader:{self.serv.server_id}] stepping down: {peer} has higher term {resp.term}")
                    self.serv.step_down(resp.term)

    def _start_sync(self, peer):
        with self._inflight_lock:
            if peer in self._syncing:
                return
            self._syncing.add(peer)
        self.pool.submit(self._sync_follower, peer)

    def _sync_follower(self, peer):
        """Bring a follower that missed uploads up to date: send it every file whose version
        differs from ours, then our commit point."""
        stub = self.serv.peer_stubs[peer]
        try:
            remote = stub.GetFileVersions(file_service_pb2.FileVersionsRequest(), timeout=SYNC_RPC_TIMEOUT)
            # Copy our state under the upload lock so no commit lands halfway through
            with self.serv.upload_lock, self.serv.lock:
                if self.serv.role != "leader":
                    return
                term = self.serv.current_term
                commit_index, commit_term = self.serv.commit_index, self.serv.commit_term
                versions = {name: ver for name, ver in self.serv.meta["file_versions"].items()
                            if remote.versions.get(name, 0) != ver}
                files = {}
                for name in versions:
                    with open(os.path.join(DATA_DIR, name), "rb") as f:
                        files[name] = f.read()

            print(f"[Leader:{self.serv.server_id}] syncing {peer} from index {remote.commit_index} "
                  f"to {commit_index}: sending {len(files)} file(s)")
            for name, data in files.items():
                resp = stub.InstallFile(file_service_pb2.InstallFileRequest(
                    term=term, filename=name, data=data, version=versions[name]), timeout=SYNC_RPC_TIMEOUT)
                if not resp.success:
                    return
            stub.FinishSync(file_service_pb2.FinishSyncRequest(
                term=term, commit_index=commit_index, commit_term=commit_term), timeout=SYNC_RPC_TIMEOUT)
        except grpc.RpcError as e:
            print(f"[Leader:{self.serv.server_id}] sync of {peer} failed: {e.code().name}")
        except Exception as e:
            print(f"[Leader:{self.serv.server_id}] sync of {peer} failed: {e}")
        finally:
            with self._inflight_lock:
                self._syncing.discard(peer)
