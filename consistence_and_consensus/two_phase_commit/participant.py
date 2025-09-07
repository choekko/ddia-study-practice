from __future__ import annotations

from time import sleep
from typing import Dict, Optional
import os
from .message import PrepareReq, PrepareResp, Decision
from .log import Log

class Participant:
    """
    - 계좌 장부를 들고 있고,
    - prepare 에서 잔고 검증 후 YES/NO 투표
    - commit 에서 실제 반영, abort 에서 되돌리기
    - crash / recover 시 2PC의 'blocking' 특성(코디네이터 의존)도 보여줌
    """
    def __init__(self, name: str, initial_accounts: Dict[str, int], log_dir="./_2pc_logs"):
        self.name = name
        self.accounts: Dict[str, int] = dict(initial_accounts)
        self.pending: Dict[str, Dict[str, int]] = {}  # txid -> delta
        self.state: Dict[str, str] = {}  # txid -> INIT/READY/COMMITTED/ABORTED
        self.alive: bool = True
        self.log = Log(os.path.join(log_dir, f"participant_{name}.log"))
        self.coordinator: Optional["Coordinator"] = None  # 복구 시 질의용(데모 편의)

    # --- 내부 유틸 ---
    def _ensure_alive(self):
        if not self.alive:
            raise RuntimeError(f"[{self.name}] is CRASHED")

    def balance(self, account: str) -> int:
        return self.accounts.get(account, 0)

    def pretty_accounts(self) -> str:
        return ", ".join([f"{k}:{v}" for k, v in sorted(self.accounts.items())])

    # --- 2PC 핸들러 ---
    def on_prepare(self, req: PrepareReq) -> PrepareResp:
        self._ensure_alive()
        # 잔고 체크
        for acct, d in req.delta.items():
            cur = self.accounts.get(acct, 0)
            if cur + d < 0:
                # 부족하면 거부
                self.log.append({"event": "VOTE", "txid": req.txid, "vote": "NO", "delta": req.delta})
                print(f"  - [{self.name}] PREPARE NO (insufficient funds on '{acct}', have={cur}, need={-d})")
                return PrepareResp(txid=req.txid, vote="NO")

        # 임시 보류(prepare OK)
        self.pending[req.txid] = dict(req.delta)
        self.state[req.txid] = "READY"
        self.log.append({"event": "PREPARED", "txid": req.txid, "delta": req.delta})
        print(f"  - [{self.name}] PREPARE YES (delta={req.delta})")
        return PrepareResp(txid=req.txid, vote="YES")

    def on_commit(self, decision: Decision) -> None:
        self._ensure_alive()
        assert decision.kind == "COMMIT"
        delta = self.pending.get(decision.txid, {})
        # 실제 반영
        for acct, d in delta.items():
            self.accounts[acct] = self.accounts.get(acct, 0) + d
        self.pending.pop(decision.txid, None)
        self.state[decision.txid] = "COMMITTED"
        self.log.append({"event": "COMMIT", "txid": decision.txid})
        print(f"  - [{self.name}] COMMIT applied")

    def on_abort(self, decision: Decision) -> None:
        self._ensure_alive()
        assert decision.kind == "ABORT"
        # 보류분 폐기
        self.pending.pop(decision.txid, None)
        self.state[decision.txid] = "ABORTED"
        self.log.append({"event": "ABORT", "txid": decision.txid})
        print(f"  - [{self.name}] ABORT done (discarded pending)")

    # --- 크래시/복구 ---
    def crash(self) -> None:
        self.alive = False
        print(f"  - [{self.name}] *** CRASHED ***")

    def recover(self) -> None:
        self.alive = True
        print(f"  - [{self.name}] *** RECOVERING ***")
        # 로그 재구성: PREPARED인데 최종 결론 없는 tx가 있으면 코디네이터에 물어봄(블로킹)
        records = self.log.load()
        prepared_open: Dict[str, dict] = {}
        decided: Dict[str, str] = {}
        for rec in records:
            if rec["event"] == "PREPARED":
                prepared_open[rec["txid"]] = rec
            elif rec["event"] in ("COMMIT", "ABORT"):
                decided[rec["txid"]] = rec["event"]

        for txid, rec in list(prepared_open.items()):
            if txid in decided:
                # 이미 결론 난 트랜잭션(재실행 불필요)
                continue
            print(f"  - [{self.name}] BLOCKED on tx={txid}. Asking coordinator for decision...")

            sleep(3) # log 변화를 볼 수 있도록 3초 대기

            if not self.coordinator:
                print(f"  - [{self.name}] Coordinator unknown → still BLOCKED")
                continue
            outcome = self.coordinator.get_decision(txid)
            if outcome == "COMMIT":
                # 보류분 읽어 반영
                delta = rec["delta"]
                for acct, d in delta.items():
                    self.accounts[acct] = self.accounts.get(acct, 0) + d
                self.log.append({"event": "COMMIT", "txid": txid})
                self.state[txid] = "COMMITTED"
                print(f"  - [{self.name}] Learned COMMIT from coordinator → applied")
            elif outcome == "ABORT":
                self.log.append({"event": "ABORT", "txid": txid})
                self.state[txid] = "ABORTED"
                print(f"  - [{self.name}] Learned ABORT from coordinator → discarded")
            else:
                print(f"  - [{self.name}] Coordinator has no decision yet → still BLOCKED")
