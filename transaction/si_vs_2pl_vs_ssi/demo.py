from .si import demo_SI
from .ssi import demo_SSI
from .two_phase_locking import demo_2pl


def run():
    print("=== [Isolation] SI vs SSI vs 2PL Serializable ===")

    print("[SI: write skew 재현]")
    print(" ->", demo_SI())

    print("[SSI: 상호 rw 사이클 → 나중 커미터 abort]")
    print(" ->", demo_SSI())

    print("[2PL + 서술 잠금 락 흉내: write skew 방지]")
    print(" ->", demo_2pl())

if __name__ == "__main__":
    run()