from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import os
import time
from .message import PrepareReq, Decision
from .log import Log
from .participant import Participant


class Coordinator:
    def __init__(self, participants: List[Participant], log_dir="./_2pc_logs"):
        self.participants = participants
        self.decisions: Dict[str, str] = {}  # txid -> "COMMIT"/"ABORT"
        self.log = Log(os.path.join(log_dir, "coordinator.log"))
        # 각 참가자에게 coordinator 참조 넘김(데모 편의: 복구 시 질의)
        for p in self.participants:
            p.coordinator = self

    def get_decision(self, txid: str) -> Optional[str]:
        # 복구 시 참가자가 호출(블로킹 해소)
        return self.decisions.get(txid)

    def two_phase_commit(self, txid: str, plan: Dict[Participant, Dict[str, int]], timeout_sec: float = 3.0) -> str:
        """
        plan: {participant -> {account -> delta}}
        """
        print(f"[COORD] === TPC START tx={txid} ===")
        self.log.append({"event": "BEGIN", "txid": txid})

        # 1) PREPARE 단계
        votes: List[Tuple[str, str]] = []
        deadline = time.time() + timeout_sec
        for p, delta in plan.items():
            try:
                if time.time() > deadline:
                    raise TimeoutError("prepare timeout")
                resp = p.on_prepare(PrepareReq(txid=txid, delta=delta))
                votes.append((p.name, resp.vote))
            except Exception as e:
                print(f"[COORD] Prepare to {p.name} failed: {e}")
                votes.append((p.name, "NO"))

        all_yes = all(v == "YES" for _, v in votes)
        print(f"[COORD] Votes: {votes} -> all_yes={all_yes}")

        # 2) 결정 및 브로드캐스트
        if all_yes:
            self.decisions[txid] = "COMMIT"
            self.log.append({"event": "DECISION", "txid": txid, "decision": "COMMIT"})
            print(f"[COORD] DECISION = COMMIT (recorded)")
            # 커밋 전달
            for p in plan.keys():
                try:
                    p.on_commit(Decision(txid=txid, kind="COMMIT"))
                except Exception as e:
                    print(f"[COORD] Commit to {p.name} failed (will rely on recovery): {e}")
            self.log.append({"event": "END", "txid": txid, "outcome": "COMMIT"})
            print(f"[COORD] === TPC END tx={txid} outcome=COMMIT ===\n")
            return "COMMIT"
        else:
            self.decisions[txid] = "ABORT"
            self.log.append({"event": "DECISION", "txid": txid, "decision": "ABORT"})
            print(f"[COORD] DECISION = ABORT (recorded)")
            # 어보트 전달
            for p in plan.keys():
                try:
                    p.on_abort(Decision(txid=txid, kind="ABORT"))
                except Exception as e:
                    print(f"[COORD] Abort to {p.name} failed: {e}")
            self.log.append({"event": "END", "txid": txid, "outcome": "ABORT"})
            print(f"[COORD] === TPC END tx={txid} outcome=ABORT ===\n")
            return "ABORT"
