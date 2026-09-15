import grpc
from concurrent import futures
import threading
import time
import random

from proto.generated.file_service import file_service_pb2

# Election and heartbeat timing. All timers use time.monotonic(): a wall-clock jump
# (NTP correction, host suspend) must not trigger or suppress elections.
ELECTION_TIMEOUT = (3.0, 7.0)
HEARTBEAT_INTERVAL = 1.0
# Per-RPC deadlines. These must stay well below the election timeout so a dead or hung
# peer can never delay the leader's heartbeats to the healthy ones.
HEARTBEAT_RPC_TIMEOUT = 0.5
VOTE_RPC_TIMEOUT = 1.0
TIMER_TICK = 0.05


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
        req = file_service_pb2.VoteRequest(term=term, candidate_id=self.serv.server_id, pre_vote=pre_vote)
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
                req = file_service_pb2.HeartbeatRequest(leader_id=self.serv.advertise_addr, term=term)

            # Fire all heartbeats concurrently without waiting on any of them
            for peer, stub in self.serv.peer_stubs.items():
                with self._inflight_lock:
                    if peer in self._inflight:
                        continue
                    self._inflight.add(peer)
                fut = stub.Heartbeat.future(req, timeout=HEARTBEAT_RPC_TIMEOUT)
                fut.add_done_callback(lambda f, p=peer, t=term: self._on_heartbeat_reply(p, t, f))

    def _on_heartbeat_reply(self, peer, term, fut):
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

        if not resp.ok and resp.term > term:
            with self.serv.lock:
                if resp.term > self.serv.current_term:
                    print(f"[Leader:{self.serv.server_id}] stepping down: {peer} has higher term {resp.term}")
                    self.serv.step_down(resp.term)
