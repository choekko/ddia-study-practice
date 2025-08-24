def naive_kv_transfer(crash_midway: bool = False):
    """
    '두 문서(A, B)를 따로 저장하는' 순진한 KV 모델을 흉내.
    - 트랜잭션이 없는 경우를 테스트
    - 중간에 예외가 나면 절반만 반영되는 '찢어진 쓰기' 상태를 보여준다.

    Returns: dict 예) {'A': 50, 'B': 100}
    """
    kv = {"A": 100, "B": 100}
    try:
        kv["A"] -= 50     # 1) A 문서 먼저 저장
        if crash_midway:
            raise RuntimeError("boom")  # 2) 중간에 장애 발생
        kv["B"] += 50     # 3) B 문서 저장은 도달 못함 → 반쪽 반영
    except Exception:
        # 트랜잭션/롤백 부재 → 손실 상태를 그대로 방치
        pass
    return kv
