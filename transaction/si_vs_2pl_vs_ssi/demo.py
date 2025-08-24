from .si import demo_SI
from .ssi import demo_SSI
from .two_phase_locking import demo_2pl


def run():
    print("=== [Isolation] SI vs SSI vs 2PL Serializable ===")
    print("""
    <상황>
    - A_on = True, B_on = True (두 의사 모두 당직 중)
    - T1: "B_on이 True면 나는 off로" → A_on = False
    - T2: "A_on이 True면 나는 off로" → B_on = False
    """)

    print("[SI: write skew 재현]")
    print(" ->", demo_SI())

    print("[SSI: 상호 rw 사이클 → 나중 커미터 abort]")
    print(" ->", demo_SSI())

    print("[2PL + 서술 잠금 락 흉내: write skew 방지]")
    print(" ->", demo_2pl())

if __name__ == "__main__":
    run()