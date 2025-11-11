import grpc
from concurrent import futures
import threading
import time
import random

from proto.generated.file_service import file_service_pb2, file_service_pb2_grpc
from .utils import save_meta

# Election and heartbeat timing
ELECTION_TIMEOUT = (3.0, 7.0)
HEARTBEAT_INTERVAL = 1.0


class RaftManager:
    def __init__(self, serv):
        # serv is the FileServiceServicer instance
        self.serv = serv

    def election_timer(self):
        # small startup jitter
        initial_jitter = random.uniform(0.0, 1.0)
        time.sleep(initial_jitter)
        while True:
            timeout = random.uniform(*ELECTION_TIMEOUT)
            start = time.time()

            while time.time() - start < timeout:
                with self.serv.lock:
                    if time.time() - self.serv.last_heartbeat < timeout:
                        break
                time.sleep(0.05)
            else:
                with self.serv.lock:
                    if self.serv.role == "leader":
                        continue
                self.start_election()

    def start_election(self):
        with self.serv.lock:
            self.serv.current_term += 1
            self.serv.role = "candidate"
            self.serv.voted_for = self.serv.server_id
            self.serv.meta["term"] = self.serv.current_term
            self.serv.meta["voted_for"] = self.serv.voted_for
            try:
                save_meta(self.serv.meta)
            except Exception:
                pass
            term = self.serv.current_term

        votes = 1
        print(f"[Election] {self.serv.server_id} starting election for term {term}")

        majority = (self.serv.cluster_size // 2) + 1

        def contact_peer(p):
            for attempt in range(2):
                try:
                    ch = grpc.insecure_channel(p)
                    try:
                        grpc.channel_ready_future(ch).result(timeout=0.8)
                    except Exception:
                        time.sleep(0.05 + random.uniform(0, 0.05))
                        continue
                    stub = file_service_pb2_grpc.FileServiceStub(ch)
                    try:
                        return stub.RequestVote(file_service_pb2.VoteRequest(term=term, candidate_id=self.serv.server_id), timeout=1.5)
                    except Exception:
                        time.sleep(0.05)
                        continue
                except Exception:
                    time.sleep(0.05)
                    continue
            return None

        futures_map = {}
        with futures.ThreadPoolExecutor(max_workers=max(1, len(self.serv.peers))) as exc:
            for peer in self.serv.peers:
                futures_map[exc.submit(contact_peer, peer)] = peer

            try:
                done, not_done = futures.wait(list(futures_map.keys()), timeout=3)
            except Exception:
                done = set()
                not_done = set(futures_map.keys())

            remaining_possible = len(self.serv.peers)
            for fut in done:
                peer = futures_map.get(fut)
                remaining_possible -= 1
                try:
                    resp = fut.result()
                except Exception as e:
                    print(f"[Election] contacting {peer} raised: {e}")
                    resp = None

                if resp is None:
                    print(f"[Election] no response from {peer}")
                else:
                    if getattr(resp, 'term', None) is not None and resp.term > term:
                        with self.serv.lock:
                            self.serv.current_term = resp.term
                            self.serv.role = 'follower'
                            self.serv.voted_for = None
                            self.serv.meta['term'] = self.serv.current_term
                            self.serv.meta['voted_for'] = self.serv.voted_for
                            try:
                                save_meta(self.serv.meta)
                            except Exception:
                                pass
                        print(f"[Election] stepping down: peer {peer} has higher term {resp.term}")
                        return
                    if resp.vote_granted:
                        votes += 1
                        print(f"[Election] received vote from {peer} (term={resp.term})")
                    else:
                        print(f"[Election] vote denied by {peer} (term={resp.term})")

                if votes >= majority:
                    break

            for fut in not_done:
                peer = futures_map.get(fut)
                remaining_possible -= 1
                fut.cancel()
                print(f"[Election] peer {peer} did not respond in time and will be treated as unreachable")

        with self.serv.lock:
            if votes >= majority and self.serv.current_term == term and self.serv.role == "candidate":
                self.serv.role = "leader"
                # Use RPC env variables for advertised leader address if available
                try:
                    import os
                    rpc_addr = f"{os.getenv('RPC_HOST', '0.0.0.0')}:{os.getenv('RPC_PORT', '50051')}"
                except Exception:
                    rpc_addr = None
                self.serv.leader_host = rpc_addr or self.serv.leader_host or self.serv.server_id
                self.serv.meta["last_leader"] = self.serv.leader_host
                try:
                    save_meta(self.serv.meta)
                except Exception:
                    pass
                print(f"[Leader] {self.serv.server_id} WON elected leader (term {term})")
                threading.Thread(target=self.send_heartbeats, daemon=True).start()
            else:
                print(f"[Election] {self.serv.server_id} lost election (votes={votes})")
                self.serv.role = "follower"

    def send_heartbeats(self):
        while True:
            print(f"I am {self.serv.server_id} and am I leader? {self.serv.role} and I think that {self.serv.leader_host} is the leader")
            with self.serv.lock:
                if self.serv.role != "leader":
                    break
                term = self.serv.current_term
                leader_id = self.serv.leader_host or self.serv.server_id
            for peer in self.serv.peers:
                try:
                    channel = grpc.insecure_channel(peer)
                    stub = file_service_pb2_grpc.FileServiceStub(channel)
                    stub.Heartbeat(file_service_pb2.HeartbeatRequest(
                        leader_id=leader_id, term=term
                    ))
                except Exception as e:
                    print(f"[Leader:{self.serv.server_id}] heartbeat to {peer} failed: {e}")
            time.sleep(HEARTBEAT_INTERVAL)
