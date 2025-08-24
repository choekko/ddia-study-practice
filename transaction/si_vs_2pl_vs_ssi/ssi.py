from threading import Lock

class SSIStore:
    """
    연습용 Serializable Snapshot Isolation(SSI) 시뮬레이터.
    목표: 'SI에서는 발생하는 write skew'를 'SSI에서는 한 쪽 abort'로 막는 걸 보여주기.

    핵심 아이디어(간이 구현):
    1) MVCC 스냅샷 읽기 + 'SIREAD(읽기 발자국)'을 남긴다. sireads[key]에 읽은 트랜잭션들의 tid를 기록.
    2) 커밋 시, 내가 쓰는 key를 과거에 '스냅샷으로 읽은' 동시 트랜잭션과의 관계를
       'rw-edge (reader -> writer)'로 기록한다.
       - 즉, reader가 먼저 읽고, writer가 나중에 그 키를 쓴 관계.
    3) 커밋 직전 '위험 구조(dangerous structure)'를 간단히 감지:
       - 현재 트랜잭션 T에 대해 'T로 들어오는 rw-edge'가 존재하고,
         또한 'T에서 나가는 rw-edge'가 이미 존재하면(= T가 pivot),
         교착 위험이 있어 T를 abort 한다.
       - 또는 상호 rw(T<->U) 사이클이 발견되면 나중 커미터를 abort.
    4) 데모 용도이므로 완전한 SSI와 100% 동일하지는 않지만,
       DDIA 7장의 'write skew 방지' 포인트를 직관적으로 재현한다.
    """

    def __init__(self):
        # 버전 테이블: key -> [(start_tid, end_tid, value)]
        self.data = {}
        # 전역 timestamp/tx id
        self._next_tid = 1
        self._tid_lock = Lock()

        # SIREAD(읽기 발자국): key -> set(tid)
        # - 진짜 DB처럼 복잡한 수명 관리 대신, 데모에 필요한 동안은 유지한다.
        self.sireads = {}

        # 트랜잭션 메타: tid -> {"start": ts, "commit": ts|None, "active": bool, "read_set": set, "write_set": dict}
        self.txn = {}

        # rw-edge 집합: (reader_tid, writer_tid)
        # - writer 커밋 시 추가한다.
        self.rw_edges = set()

    # ---------- 공용 유틸 ----------

    def _alloc_tid(self) -> int:
        with self._tid_lock:
            tid = self._next_tid
            self._next_tid += 1
            return tid

    def _now_ts(self) -> int:
        # 단순히 다음 tid가 곧 '미래 시각'이라 보고 조회에 사용
        return self._next_tid

    # ---------- MVCC 읽기/쓰기 기본 ----------

    def _read_version(self, key: str, ts: int):
        """
        MVCC 스냅샷 읽기: 스냅샷 시점(ts)에서 볼 수 있는 최신 버전.
        """
        versions = self.data.get(key, [])
        cand = None
        for s, e, v in versions:
            if s <= ts and (e is None or e > ts):
                if cand is None or s > cand[0]:
                    cand = (s, e, v)
        return cand[2] if cand else None

    def _write_commit(self, write_set: dict, commit_tid: int):
        """
        쓰기 커밋: 열린 최신 버전을 닫고 새 버전을 연다.
        """
        for key, value in write_set.items():
            versions = self.data.setdefault(key, [])
            # 열린 최신 버전 닫기
            if versions:
                for i in range(len(versions) - 1, -1, -1):
                    s, e, v = versions[i]
                    if e is None:
                        versions[i] = (s, commit_tid, v)
                        break
            # 새 버전 생성
            versions.append((commit_tid, None, value))

    # ---------- 트랜잭션 라이프사이클 ----------

    def begin(self):
        """
        트랜잭션 시작: 스냅샷 시점을 ts로 고정한다.
        """
        ts = self._alloc_tid()
        t = SSITransaction(self, ts)
        # 메타 등록
        self.txn[ts] = {
            "start": ts,
            "commit": None,
            "active": True,
            "read_set": set(),
            "write_set": {},
        }
        return t

    # ---------- SSI 전용 보조 ----------

    def _add_siread(self, tid: int, key: str):
        """
        'tid가 key를 스냅샷으로 읽었다'는 발자국을 기록.
        - 나중에 누군가 key를 '쓴 채로 커밋'하면 (reader -> writer) rw-edge를 추가할 근거가 된다.
        """
        self.sireads.setdefault(key, set()).add(tid)
        self.txn[tid]["read_set"].add(key)

    def _overlap(self, t1: int, t2: int) -> bool:
        """
        트랜잭션 시간 구간이 겹치는지(동시성) 판단.
        간이 규칙:
        - t.start = start_ts
        - t.commit = commit_ts or None(아직 미커밋)
        - 커밋 시점의 트랜잭션을 기준으로, 읽은 쪽과 쓰는 쪽이 '시간상 겹쳤다'면 True.
        """
        s1 = self.txn[t1]["start"]
        c1 = self.txn[t1]["commit"]  # None이면 아직 active
        s2 = self.txn[t2]["start"]
        c2 = self.txn[t2]["commit"]  # 이번에 커밋 중인 트랜잭션이면 곧 값이 들어감

        # [s1, c1 or +∞) 와 [s2, c2 or +∞) 가 겹치면 True
        end1 = c1 if c1 is not None else float("inf")
        end2 = c2 if c2 is not None else float("inf")
        return not (end1 < s2 or end2 < s1)

    def _add_rw_edges_for_writer(self, writer_tid: int, write_keys: set):
        """
        writer_tid가 write_keys를 커밋하려 할 때,
        해당 key를 읽은(reader) 트랜잭션들과의 rw-edge를 추가한다.
        """
        for key in write_keys:
            readers = self.sireads.get(key, set())
            for r_tid in readers:
                if r_tid == writer_tid:
                    continue
                # 시간 구간이 겹치면(동시성) reader->writer 간선 추가
                if self._overlap(r_tid, writer_tid):
                    self.rw_edges.add((r_tid, writer_tid))

    def _has_dangerous_structure(self, pivot_tid: int) -> bool:
        """
        위험 구조 감지(간이):
        - '누군가 -> pivot' (incoming rw-edge) 가 있고,
        - 'pivot -> 누군가' (outgoing rw-edge) 도 이미 존재하면 True.
        - 즉 pivot이 중간에 끼어 사이클에 연루될 위험이 있다고 보고 abort.
        """
        incoming = any(dst == pivot_tid for (_, dst) in self.rw_edges)
        outgoing = any(src == pivot_tid for (src, _) in self.rw_edges)
        return incoming and outgoing

    # ---------- 커밋/어보트 ----------

    def _commit(self, tid: int):
        """
        커밋 절차(간이 SSI):
        1) 우선 커밋 타임스탬프 할당
        2) 내가 쓴 key에 대해 siread를 조사해 reader->tid rw-edge 추가
        3) 추가 결과 pivot이 되는지 검사 → 위험하면 abort
        4) 안전하면 버전 테이블에 반영
        """
        # 이미 끝난 트랜잭션이면 무시
        meta = self.txn[tid]
        if not meta["active"]:
            return False, "already finished"

        # 1) commit_ts 할당
        commit_ts = self._alloc_tid()
        # 커밋 전 검사에 사용될 값을 메타에 채워둔다(구간 겹침 계산용)
        meta["commit"] = commit_ts

        # 2) 내가 쓰는 key들로 rw-edges 추가
        write_keys = set(meta["write_set"].keys())
        self._add_rw_edges_for_writer(tid, write_keys)

        # 3) pivot 검사: 이미 존재하는 '나가는 간선'과 '들어오는 간선'이 동시에 있으면 위험
        if self._has_dangerous_structure(pivot_tid=tid):
            # 실패 → 되돌림
            meta["commit"] = None
            meta["active"] = False
            return False, "ssi: dangerous structure -> abort"

        # 4) 안전하면 실제 데이터 커밋
        self._write_commit(meta["write_set"], commit_ts)
        meta["active"] = False
        return True, commit_ts

    def _abort(self, tid: int, reason: str):
        """
        Abort: 상태 플래그만 정리(데모 목적상 데이터 변경 없음).
        siread은 간소화를 위해 GC하지 않아도 데모가 동작한다.
        """
        meta = self.txn[tid]
        meta["active"] = False
        return False, reason


class SSITransaction:
    """
    SSI 트랜잭션 객체.
    - MVCC 스냅샷 읽기 + SIREAD 발자국 남기기
    - commit 전에 SSI 위험 구조를 검사(간이)하여 필요시 abort
    """
    def __init__(self, store: SSIStore, ts: int):
        self.store = store
        self.ts = ts
        # 로컬 write set은 메타 안에도 복사되지만, 여기에도 유지
        self.write_set = {}
        self.active = True

    def read(self, key: str):
        """
        스냅샷 읽기 + siread 표시.
        - '나중에 누가 이 key를 쓰고 커밋하면' → (나 -> 그 사람) rw-edge 후보가 된다.
        """
        # 내가 이미 쓴 값이 있으면 그걸 우선 반환(쓰기-읽기 재정렬)
        if key in self.write_set:
            val = self.write_set[key]
        else:
            val = self.store._read_version(key, self.ts)
        # siread 등록 (내가 이 key를 읽었다고 발자국 남김)
        self.store._add_siread(self.ts, key)
        return val

    def write(self, key: str, value):
        """
        지연 쓰기: 실제 데이터 갱신은 commit 시점에만 수행.
        - 내 메타의 write_set에도 동기화해둔다.
        """
        self.write_set[key] = value
        self.store.txn[self.ts]["write_set"][key] = value

    def commit(self):
        """
        SSI 커밋: 위험 구조 감지(간이) 후 커밋 or 어보트.
        - 양측이 서로의 읽은 키를 쓰는 '상호 rw' 상황에서는 '나중 커미터'가 abort됨.
        """
        if not self.active:
            return False, "already finished"
        ok, info = self.store._commit(self.ts)
        self.active = False
        return ok, info

    def abort(self, reason="manual abort"):
        if not self.active:
            return False, "already finished"
        self.active = False
        return self.store._abort(self.ts, reason)


def demo_SSI():
    """
    SSI로 같은 write skew 패턴을 돌려보면,
    - 둘 다 서로가 읽은 키를 썼기 때문에 상호 rw 관계가 생김
    - '나중 커미터'를 abort하여 사이클을 끊는다(간이 규칙)
    """
    store = SSIStore()

    # 초기 상태: 둘 다 on
    t0 = store.begin()
    t0.write("A_on", True)
    t0.write("B_on", True)
    ok, _ = t0.commit()
    assert ok

    # 두 트랜잭션 동시에 시작
    t1 = store.begin()
    t2 = store.begin()

    # 스냅샷으로 읽되 SIREAD 남김
    assert t1.read("B_on")
    assert t2.read("A_on")

    # 각자 자신의 키만 끔
    t1.write("A_on", False)
    t2.write("B_on", False)

    # '나중 커미터'가 abort되도록 설계됨(상호 rw 사이클)
    ok1, info1 = t1.commit()
    ok2, info2 = t2.commit()

    # 최신 시점에서 값 확인
    now = store._now_ts()
    A = store._read_version("A_on", now)
    B = store._read_version("B_on", now)
    return {
        "A_on": A,
        "B_on": B,
        "t1_commit": (ok1, info1),
        "t2_commit": (ok2, info2),
        "rw_edges": list(store.rw_edges),
    }
