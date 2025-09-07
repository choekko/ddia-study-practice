from __future__ import annotations
from typing import List
from .participant import Participant
from .cordinator import Coordinator
from .message import PrepareReq, PrepareResp


def print_balances(ps: List[Participant], title: str):
    print(f"\n== {title} ==")
    for p in ps:
        print(f"  {p.name}: {p.pretty_accounts()}")
    print()


def reset_logs():
    # 데모를 반복해도 로그 디렉터리를 유지해, 복구 시뮬레이션이 자연스럽게 보이도록 함.
    # 필요하면 주석 해제하여 매 실행마다 초기화할 수 있음.
    # import shutil
    # shutil.rmtree("./_2pc_logs", ignore_errors=True)
    pass


def demo():
    reset_logs()

    # 참가자 2명: BankA, BankB
    A = Participant("BankA", {"alice": 500})
    B = Participant("BankB", {"bob": 300})
    C = Participant("BankC", {"carol": 0})  # 시나리오 2에서 부족분 유도용
    coord = Coordinator([A, B, C])

    # 시나리오 1: 정상 커밋 (alice -100 @A, bob +100 @B)
    print("### 시나리오 1: 정상 커밋\n")
    print_balances([A, B], "초기 잔고")

    print("== 계획: alice -> bob 으로 100 보내기 ==", end="\n\n")

    outcome = coord.two_phase_commit(
        txid="tx-001",
        plan={
            A: {"alice": -100},
            B: {"bob": +100},
        },
    )
    print_balances([A, B], f"트랜잭션 결과 = {outcome}")

    # 시나리오 2: PREPARE 단계에서 거부 → ABORT (carol -50 @C) (잔고 0 → 거부)
    print("### 시나리오 2: 준비(prepare)에서 거부되어 ABORT\n")
    print_balances([B, C], "초기 잔고")

    print("== 계획: carol -> bob 으로 50 보내기 ==", end="\n\n")

    outcome = coord.two_phase_commit(
        txid="tx-002",
        plan={
            C: {"carol": -50},   # 잔고 부족으로 NO
            B: {"bob": +50},
        },
    )
    print_balances([B, C], f"트랜잭션 결과 = {outcome}")

    # 시나리오 3: 한 참가자가 PREPARE YES 직후 크래시, 나중에 복구하여 커밋 학습
    print("### 시나리오 3: PREPARE 이후 크래시 & 복구 (2PC의 blocking 복구)\n")
    print_balances([A, B], "시작 잔고")

    print("== 계획: alice -> bob 으로 50 보내기 ==", end="\n\n")

    # BankB가 PREPARE YES 한 직후 크래시하게 연출
    def prepare_and_crash(req: PrepareReq) -> PrepareResp:
        # BankB의 메서드를 후킹(hook)하여 첫 prepare에서만 크래시 유발
        B._ensure_alive()
        # 정상 로직으로 YES 준비
        for acct, d in req.delta.items():
            cur = B.accounts.get(acct, 0)
            if cur + d < 0:
                B.log.append({"event": "VOTE", "txid": req.txid, "vote": "NO", "delta": req.delta})
                print(f"  - [BankB] PREPARE NO (insufficient funds)")
                return PrepareResp(txid=req.txid, vote="NO")
        B.pending[req.txid] = dict(req.delta)
        B.state[req.txid] = "READY"
        B.log.append({"event": "PREPARED", "txid": req.txid, "delta": req.delta})
        print(f"  - [BankB] PREPARE YES (delta={req.delta})")
        # 곧바로 크래시
        B.crash()
        # 다음부터는 원래 on_prepare로 되돌림
        B.on_prepare = Participant.on_prepare.__get__(B, Participant)
        return PrepareResp(txid=req.txid, vote="YES")

    # BankB의 on_prepare를 일시 덮어쓰기(첫 호출만)
    B.on_prepare = prepare_and_crash  # type: ignore

    outcome = coord.two_phase_commit(
        txid="tx-003",
        plan={
            A: {"alice": -50},
            B: {"bob": +50},
        },
    )
    print_balances([A, B], f"코디네이터 관점 결과 = {outcome} (BankB는 아직 크래시)")

    # 이제 BankB 복구 → 로그를 보고 코디네이터에 결정 질의 → 커밋 학습
    B.recover()
    print_balances([A, B], "BankB 복구 후(커밋 학습 반영 완료)")

    print("### 데모 종료 ###")


if __name__ == "__main__":
    demo()