from typing import Dict


def pct(xs, p):
    """리스트 xs의 p-퍼센타일 값을 계산하는 간단한 함수"""
    # xs가 비어 있으면 0.0 반환
    if not xs:
        return 0.0
    # 정렬된 복사본 준비(퍼센타일 계산은 정렬 기반)
    xs = sorted(xs)
    # p 위치(0~len-1 사이 실수 인덱스)를 선형 보간 방식으로 계산
    k = (len(xs) - 1) * (p / 100.0)
    f, c = int(k), min(int(k) + 1, len(xs) - 1)
    # 정수 인덱스라면 그 값 반환
    if f == c:
        return xs[f]
    # 실수 인덱스라면 f와 c 사이를 선형 보간
    return xs[f] * (c - k) + xs[c] * (k - f)

class Meter:
    """벤치마크 동안 지연/일관성 관련 지표를 수집하는 간단한 집계기"""
    def __init__(self):
        # 읽기 지연(ms) 기록 리스트
        self.read_lat = []
        # 쓰기 지연(ms) 기록 리스트
        self.write_lat = []
        # 총 읽기/쓰기 카운트
        self.reads = 0
        self.writes = 0
        # 최신값보다 과거를 읽은 횟수(복제 지연으로 인한 stale read)
        self.stale = 0
        # 내가 바로 전에 쓴 값을 못 본 횟수(세션 관점의 RYW 위반)
        self.ryw_violation = 0
        # (CouchDB) 문서 충돌 관측 횟수(_conflicts)
        self.conflicts = 0
        # 각 키에 대해 "마지막으로 쓴 버전"을 기억(간단한 버전 카운터)
        self.last_written: Dict[int, int] = {}
        # 각 키에 대해 "마지막으로 본 버전"(단조 읽기 체크 보조용)
        self.last_seen: Dict[int, int] = {}

    def report(self, model: str):
        """수집된 지표를 사람이 보기 쉬운 딕셔너리로 요약"""
        # 평균 계산 보조 함수(비어 있으면 0.0)
        def avg(a):
            return (sum(a) / len(a)) if a else 0.0
        # 결과 딕셔너리 구성(지연 평균/95백분위, stale/RYW율, 충돌 수)
        return {
            "model": model,
            "reads": self.reads,
            "writes": self.writes,
            "avg_read_ms": round(avg(self.read_lat), 3),
            "p95_read_ms": round(pct(self.read_lat, 95), 3),
            "avg_write_ms": round(avg(self.write_lat), 3),
            "p95_write_ms": round(pct(self.write_lat, 95), 3),
            "stale_read_rate": round(self.stale / self.reads, 4) if self.reads else 0.0,
            "ryw_violation_rate": round(self.ryw_violation / self.reads, 4) if self.reads else 0.0,
            "conflicts": self.conflicts,
        }
