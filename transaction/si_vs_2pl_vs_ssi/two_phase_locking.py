class RWLock:
    """
    연습용 Read/Write 락.
    - 다수 Shared(S) 또는 단일 Exclusive(X)
    - 공정성/대기열은 생략 (데모 목적)
    """
    def __init__(self):
        self.readers = 0
        self.writer = False

    def acquire_shared(self) -> bool:
        if self.writer:
            return False
        self.readers += 1
        return True

    def release_shared(self):
        assert self.readers > 0
        self.readers -= 1

    def acquire_exclusive(self) -> bool:
        if self.writer or self.readers > 0:
            return False
        self.writer = True
        return True

    def release_exclusive(self):
        assert self.writer
        self.writer = False

    def try_upgrade(self, txid=None):
        # 내가 유일한 S 보유자(readers==1)이고, 아직 X가 없을 때만
        if self.writer is not None or self.readers != 1:
            return False
        # 공백 없이 S→X 승격
        self.readers = 0
        self.writer = txid or True
        return True


class LockManager:
    """
    키 이름(혹은 서술 잠금 이름)별로 RWLock을 관리.
    - 2PL 흉내를 내는 데 사용.
    """
    def __init__(self):
        self.locks = {}  # name -> RWLock

    def _get(self, name: str) -> RWLock:
        if name not in self.locks:
            self.locks[name] = RWLock()
        return self.locks[name]

    def try_acquire(self, name: str, mode: str) -> bool:
        lk = self._get(name)
        if mode == "S":
            return lk.acquire_shared()
        if mode == "X":
            return lk.acquire_exclusive()
        raise ValueError("mode must be 'S' or 'X'")

    def release(self, name: str, mode: str):
        lk = self._get(name)
        if mode == "S":
            lk.release_shared()
        elif mode == "X":
            lk.release_exclusive()
        else:
            raise ValueError("mode must be 'S' or 'X'")

    def try_upgrade(self, name, txid=None):
        return self._get(name).try_upgrade(txid)

def demo_2pl():
    """
    2PL + 서술 잠금 흉내로 write skew를 방지하는 예.

    아이디어:
    - '당직자가 존재한다'는 서술 잠금에 대해 S락으로 점검한 뒤,
      off로 바꾸려면 X락으로 승격(업그레이드)해야 한다.
    - 두 트랜잭션이 동시에 업그레이드하려 하면 한쪽은 실패/대기 → 결국 한 쪽만 성공.

    단순화를 위해: 대기 대신 '업그레이드 실패=롤백'으로 구현.
    """
    # 공유 데이터(그냥 dict)
    store = {"A_on": True, "B_on": True}
    lm = LockManager()

    # 두 트랜잭션이 동시에 '누군가 on-call인가?' 서술 잠금을 읽기(S)
    assert lm.try_acquire("pred:on_call_exists", "S")
    assert lm.try_acquire("pred:on_call_exists", "S")

    # T1: 업그레이드 시도 → 실패(동시 S 보유자 2명이므로 readers!=1) → 롤백
    t1_upgraded = lm.try_upgrade("pred:on_call_exists", txid="T1")
    t1_committed = False
    if not t1_upgraded:
        lm.release("pred:on_call_exists", "S")  # T1 포기

    # T2: 이제 자신만 S를 보유(readers==1)이므로 업그레이드 성공 → 쓰기 진행
    t2_upgraded = lm.try_upgrade("pred:on_call_exists", txid="T2")
    t2_committed = False
    if t2_upgraded:
        if lm.try_acquire("B_on", "X"):
            store["B_on"] = False
            t2_committed = True
            lm.release("B_on", "X")
        lm.release("pred:on_call_exists", "X")

    return {"final": store, "t1_committed": t1_committed, "t2_committed": t2_committed}