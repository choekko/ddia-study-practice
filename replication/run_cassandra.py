import random
import time

from meter import Meter
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel

def run_cassandra(args):
    """Cassandra에서 ConsistencyLevel 조합(W/R)을 바꿔가며 측정"""

    # 문자열을 ConsistencyLevel 상수로 변환하는 헬퍼
    def cl_from(s):
        return getattr(ConsistencyLevel, s.upper())

    # 호스트 리스트를 쉼표로 분리하여 배열 생성
    hosts = [h.strip() for h in args.hosts.split(",")]
    # 사용자/비밀번호가 있으면 인증 객체 구성
    auth = None

    if args.username and args.password:
        auth = PlainTextAuthProvider(username=args.username, password=args.password)
    # 클러스터 객체 생성(기본 포트 9042, 첫 노드로부터 메타데이터 수집)
    cluster = Cluster(hosts, auth_provider=auth)
    # 세션 획득(키스페이스 선택 전에 system으로 연결됨)
    session = cluster.connect()

    # 키스페이스 생성(없으면 생성, replication_factor는 테스트용 SimpleStrategy)
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {args.keyspace} "
        f"WITH replication = {{'class':'SimpleStrategy','replication_factor':{args.rf}}};"
    )
    # 테이블 생성(없으면 생성) — k(파티션키), v/ts 필드
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {args.keyspace}.{args.table} "
        f"(k int PRIMARY KEY, v int, ts bigint);"
    )

    # 준비된 쓰기/읽기 쿼리(파라미터 바인딩)
    write = session.prepare(f"INSERT INTO {args.keyspace}.{args.table} (k, v, ts) VALUES (?, ?, ?)")
    read = session.prepare(f"SELECT v, ts FROM {args.keyspace}.{args.table} WHERE k=?")
    # ConsistencyLevel 설정(튜너블 쿼럼)
    write.consistency_level = cl_from(args.write_cl)
    read.consistency_level = cl_from(args.read_cl)

    # 메트릭 집계기 생성
    meter = Meter()
    # 난수 시드 고정
    rng = random.Random(42)

    # 총 연산 루프
    for _ in range(args.ops):
        # 쓰기/읽기 결정
        is_write = rng.random() < args.write_ratio
        # 랜덤 키 선택
        k = rng.randrange(args.keys)

        if is_write:
            # 버전 증가
            v = meter.last_written.get(k, 0) + 1
            # 쓰기 지연 시작
            t0 = time.perf_counter()
            # 파라미터 바인딩으로 쓰기 실행
            session.execute(write, (k, v, int(time.time() * 1000)))
            # 쓰기 지연 기록
            meter.write_lat.append((time.perf_counter() - t0) * 1000)
            # 쓰기 카운트 증가
            meter.writes += 1
            # 마지막 쓴 버전 갱신
            meter.last_written[k] = v
        else:
            # 읽기 지연 시작
            t0 = time.perf_counter()
            # 읽기 실행(one()으로 단일 행)
            row = session.execute(read, (k,)).one()
            # 읽기 지연 기록
            meter.read_lat.append((time.perf_counter() - t0) * 1000)
            # 읽기 카운트 증가
            meter.reads += 1
            # 결과가 없거나 v가 None이면 0으로 해석
            seen_v = row.v if row and hasattr(row, "v") and row.v is not None else 0

            # 최신값 대비 stale/RYW 체크
            latest_known = meter.last_written.get(k, 0)
            if seen_v < latest_known:
                meter.stale += 1
                if latest_known:
                    meter.ryw_violation += 1
            # last_seen 갱신
            meter.last_seen[k] = max(meter.last_seen.get(k, 0), seen_v)

    # 결과 딕셔너리 반환(모델명에 CL/W/R/RF 표기)
    return meter.report(model=f"cassandra_W{args.write_cl}_R{args.read_cl}_RF{args.rf}")
