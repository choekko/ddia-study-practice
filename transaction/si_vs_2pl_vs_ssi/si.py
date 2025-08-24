from threading import Lock

class MVCCStore:
    """
    연습용 '아주 단순한' MVCC 스토어.
    - begin() 때 스냅샷 타임스탬프(ts)를 고정.
    - commit() 때 '같은 키에 대한 write-write 충돌'만 감지.
      => 서로 다른 키만 쓰면 동시에 커밋 가능 → Write Skew가 발생할 수 있음.
    - 버전 모델: key -> [(start_tid, end_tid, value)] (시간 구간이 열린 버전들)
    """
    def __init__(self):
        self.data = {}       # key -> list[(start_tid, end_tid, value)]
        self._next_tid = 1   # 증가하는 타임스탬프/트랜잭션 ID
        self._lock = Lock()  # _next_tid 보호용

    def _alloc_tid(self) -> int:
        # 전역 타임스탬프 발급 (단조 증가)
        with self._lock:
            tid = self._next_tid
            self._next_tid += 1
            return tid

    def begin(self):
        """
        트랜잭션 시작.
        - 스냅샷 시점(ts)을 고정한다.
        """
        ts = self._alloc_tid()
        return MVCCTransaction(self, ts)

    def _read_version(self, key: str, ts: int):
        """
        스냅샷 시점 ts에서 볼 수 있는 key의 최신 버전 값을 찾는다.
        - (start <= ts < end or end is None)인 버전 중 start가 가장 큰 것
        """
        versions = self.data.get(key, [])
        cand = None
        for s, e, v in versions:
            if s <= ts and (e is None or e > ts):
                if cand is None or s > cand[0]:
                    cand = (s, e, v)
        return cand[2] if cand else None

    def _write_commit(self, txn, commit_tid: int):
        """
        트랜잭션의 write set을 커밋 타임스탬프에 맞춰 영속화.
        - 직전 최신 버전의 end를 commit_tid로 닫고
        - 새 버전 (commit_tid, None, value)를 추가
        """
        for key, value in txn.write_set.items():
            versions = self.data.setdefault(key, [])
            # 현재 열린 최신 버전(e is None)을 닫는다.
            if versions:
                for i in range(len(versions) - 1, -1, -1):
                    s, e, v = versions[i]
                    if e is None:
                        versions[i] = (s, commit_tid, v)
                        break
            # 새 버전 오픈
            versions.append((commit_tid, None, value))

    def _check_ww_conflicts(self, txn) -> bool:
        """
        간단한 WW 충돌만 감지:
        - 내가 쓰려는 key에 대해 '나보다 나중에 시작했고 아직 열린 버전'이 있으면 충돌로 본다.
        - 이 데모에서는 엄밀하지 않아도 됨(핵심은 SI에서 write skew 재현).
        """
        for key in txn.write_set:
            for s, e, _ in self.data.get(key, []):
                if s > txn.ts and e is None:  # 나보다 나중에 시작한 트랜잭션의 열린 버전
                    return True
        return False


class MVCCTransaction:
    """
    MVCC 트랜잭션 객체.
    - read(): 스냅샷 시점에서 읽기
    - write(): 로컬 write set에만 기록(지연 쓰기)
    - commit(): WW 충돌만 검사 후 반영
    """
    def __init__(self, store: MVCCStore, ts: int):
        self.store = store
        self.ts = ts
        self.write_set = {}
        self.active = True

    def read(self, key: str):
        # 내가 쓴 값이 있으면 그걸(쓰기-읽기 재정렬) 먼저 돌려준다.
        if key in self.write_set:
            return self.write_set[key]
        return self.store._read_version(key, self.ts)

    def write(self, key: str, value):
        # 실제 저장은 commit 때 수행
        self.write_set[key] = value

    def commit(self):
        # WW 충돌만 감지
        if self.store._check_ww_conflicts(self):
            self.active = False
            return False, "write-write conflict -> abort"
        commit_tid = self.store._alloc_tid()
        self.store._write_commit(self, commit_tid)
        self.active = False
        return True, commit_tid

def demo_SI():
    """
    Snapshot Isolation(SI)에서 write skew가 실제로 발생함을 보여주는 시나리오.

    설정:
    - A_on = True, B_on = True (두 의사 모두 당직 중)
    - T1: "B_on이 True면 나는 off로" → A_on = False
    - T2: "A_on이 True면 나는 off로" → B_on = False
    - 서로 '다른 키만 쓰기' 때문에 WW 충돌은 없고, 둘 다 커밋됨
    - 결과: A_on=False, B_on=False (불변식 붕괴: '최소 한 명은 on이어야 한다'가 깨짐)
    """
    store = MVCCStore()

    # 초기 상태 세팅 커밋
    tx0 = store.begin()
    tx0.write("A_on", True)
    tx0.write("B_on", True)
    ok, _ = tx0.commit()
    assert ok

    # 두 트랜잭션 동시에 시작(동시성 가정)
    t1 = store.begin()
    t2 = store.begin()

    assert t1.read("B_on")
    assert t2.read("A_on")

    # 각자 자신의 키만 끔
    t1.write("A_on", False)
    t2.write("B_on", False)

    # SI에서는 둘 다 커밋 성공 → write skew 발생
    ok1, info1 = t1.commit()
    ok2, info2 = t2.commit()

    # '가장 최신' 시점에서 결과 확인
    now_ts = store._next_tid
    return {
        "A_on": store._read_version("A_on", now_ts),
        "B_on": store._read_version("B_on", now_ts),
        "t1_commit": (ok1, info1),
        "t2_commit": (ok2, info2),
    }