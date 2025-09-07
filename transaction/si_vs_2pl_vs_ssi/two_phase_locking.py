class RWLock:
    """
    연습용 Read/Write 락.
    - 다수 Shared(S) 또는 단일 Exclusive(X)
    - 공정성/대기열은 생략 (데모 목적)
    """
    def __init__(self):
        self.readers = []
        self.writer = None

    def acquire_shared(self, t_id) -> bool:
        if self.writer:
            return False
        self.readers.append(t_id)
        return True

    def release_shared(self, t_id):
        assert len(self.readers) > 0
        self.readers.remove(t_id)

    def acquire_exclusive(self, t_id) -> bool:
        if self.writer or len(self.readers) > 1 or t_id not in self.readers:
            return False
        self.writer = t_id
        self.readers = []
        return True

    def release_exclusive(self, t_id):
        assert self.writer is t_id
        self.writer = None


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

    def try_acquire(self, name: str, mode: str, t_id: str) -> bool:
        lk = self._get(name)
        if mode == "S":
            return lk.acquire_shared(t_id)
        if mode == "X":
            return lk.acquire_exclusive(t_id)
        raise ValueError("mode must be 'S' or 'X'")

    def release(self, name: str, mode: str, t_id: str):
        lk = self._get(name)
        if mode == "S":
            lk.release_shared(t_id)
        elif mode == "X":
            lk.release_exclusive(t_id)
        else:
            raise ValueError("mode must be 'S' or 'X'")

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

    t1_committed = False
    t2_committed = False

    # 두 트랜잭션이 동시에 'on-call 인 row를 읽기 위한' 서술 공유 잠금을 얻기(S)
    assert lm.try_acquire("pred:on_call_exists", "S", "T1")
    assert lm.try_acquire("pred:on_call_exists", "S", "T2")

    # T1: on-call 인 row 의 정보를 변경(쓰기)할  수 있도록, 독점 잠금으로 업그레이드 시도
    # pred:on_call_exists 에 대한 서술 공유 잠금을 가진 다른 트랜잭션이 있으므로, 서로 대기를 하게 됨 (데드락)
    # 여기서는 upgrade가 실패한 걸로 표기
    t1_upgraded = lm.try_acquire("pred:on_call_exists", "X", "T1")
    t2_upgraded = lm.try_acquire("pred:on_call_exists", "X", "T2")

    # deadlock detection 에 의한 후처리 진행
    if not t1_upgraded and not t2_upgraded:
        # 여기서는 t1 롤백하여 t2 가 업그레이드 되도록 처리
        lm.release("pred:on_call_exists", "S", "T1")
        t1_committed = False
        t2_upgraded = lm.try_acquire("pred:on_call_exists", "X", "T2")

    if t2_upgraded:
        store["B_on"] = False
        t2_committed = True
        lm.release("pred:on_call_exists", "X", "T2")

    return {"final": store, "t1_committed": t1_committed, "t2_committed": t2_committed}