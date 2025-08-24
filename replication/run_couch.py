import random
import time

from meter import Meter
# requests: CouchDB HTTP API 호출용 세션
import requests

def run_couch(args):
    """CouchDB 2노드 간 양방향 continuous replication에서 충돌/지연 관찰"""

    # 세션 생성(커넥션 재사용)
    s = requests.Session()

    # 데이터베이스 생성(이미 존재하면 412/409 등 무시)
    def put_db(base, db):
        s.put(f"{base}/{db}")

    # 양방향 복제를 설정하는 헬퍼(각 노드에서 서로를 target으로 지정)
    def start_continuous_replication(src_base, dst_base, db):
        payload = {"source": f"{src_base}/{db}", "target": f"{dst_base}/{db}", "continuous": True}
        s.post(f"{src_base}/_replicate", json=payload, timeout=10)

    # A/B 노드에 DB 생성 후, 양방향 continuous replication 설정
    put_db(args.couch_a, args.db)
    put_db(args.couch_b, args.db)
    start_continuous_replication(args.couch_a, args.couch_b, args.db)
    start_continuous_replication(args.couch_b, args.couch_a, args.db)

    # 메트릭 집계기 생성
    meter = Meter()
    # 난수 시드 고정
    rng = random.Random(42)
    # 각 노드별 최근 _rev 캐시(갱신 시 충돌(409) 회피 위해 최신 rev를 보내야 함)
    rev_cache = {"a": {}, "b": {}}

    # "a"/"b" 문자열을 실제 베이스 URL로 변환하는 헬퍼
    def get_node(which):
        return args.couch_a if which == "a" else args.couch_b

    # 문서를 읽어오는 함수(옵션으로 conflicts 필드 포함)
    def get_doc(which, k, with_conflicts=False):
        base = get_node(which)
        params = {"conflicts": "true"} if with_conflicts else None
        r = s.get(f"{base}/{args.db}/{k}", params=params)
        if r.status_code == 200:
            return r.json()
        return None

    # 문서를 쓰는 함수(있다면 _rev 포함하여 갱신, 없으면 생성)
    def put_doc(which, k, v, rev=None):
        base = get_node(which)
        body = {"_id": str(k), "v": v, "ts": int(time.time() * 1000)}
        if rev:
            body["_rev"] = rev
        r = s.put(f"{base}/{args.db}/{k}", json=body)
        return r

    # 쓰기를 어느 노드로 보낼지 확률 설정("both"면 50:50 분산)
    write_to_a_prob = 0.5 if args.write_to == "both" else (1.0 if args.write_to == "a" else 0.0)
    # 읽기 대상 노드 고정("a" 또는 "b")
    read_from = args.read_from

    # 총 연산 루프
    for _ in range(args.ops):
        # 쓰기/읽기 결정
        is_write = rng.random() < args.write_ratio
        # 키 선택(문서 _id로 사용하기 위해 문자열로도 활용)
        k = rng.randrange(args.keys)
        sk = str(k)

        if is_write:
            # 어느 노드(a/b)에 쓸지 결정
            which = "a" if rng.random() < write_to_a_prob else "b"
            # 버전 증가
            v = meter.last_written.get(k, 0) + 1
            # 최신 rev(있으면) 조회
            rev = rev_cache[which].get(k)
            # 쓰기 요청 1회 시도
            r = put_doc(which, sk, v, rev)
            if r.status_code == 409:
                # 409(충돌)이면 최신 rev를 구해 한 번 더 시도
                doc = get_doc(which, sk)
                rev2 = doc.get("_rev") if doc else None
                r = put_doc(which, sk, v, rev2)
                # 그래도 409면 로컬 경쟁으로 인한 충돌 증가 카운트
                if r.status_code == 409:
                    meter.conflicts += 1
                else:
                    # 성공 시 rev 캐시에 최신 rev 저장
                    rev_cache[which][k] = r.json().get("rev")
            elif r.ok:
                # 첫 시도 성공 시 rev 캐시 저장
                rev_cache[which][k] = r.json().get("rev")

            # 쓰기 지연 측정은 CouchDB HTTP 왕복을 간략화해 생략
            # 간단히 0ms로 간주하지 않고 실제 시간을 넣고 싶으면 아래 2줄처럼 감싸:
            # t0 = time.perf_counter(); ...요청 수행...
            # meter.write_lat.append((time.perf_counter()-t0)*1000)
            meter.writes += 1
            meter.last_written[k] = v
        else:
            # 읽기 대상 노드 결정(고정)
            which = read_from
            # 읽기 지연 측정 시작
            t0 = time.perf_counter()
            # 문서 읽기(충돌 메타 포함)
            doc = get_doc(which, sk, with_conflicts=True)
            # 읽기 지연 기록
            meter.read_lat.append((time.perf_counter() - t0) * 1000)
            # 읽기 카운트 증가
            meter.reads += 1
            # 문서가 있으면 v, 없으면 0
            seen_v = (doc or {}).get("v", 0)

            # 복제 병행 중 충돌이 있으면 _conflicts 배열이 등장 → 충돌 카운트 증가
            if doc and "_conflicts" in doc and doc["_conflicts"]:
                meter.conflicts += 1

            # 최신값 대비 stale/RYW 체크
            latest_known = meter.last_written.get(k, 0)
            if seen_v < latest_known:
                meter.stale += 1
                if latest_known:
                    meter.ryw_violation += 1
            # last_seen 갱신
            meter.last_seen[k] = max(meter.last_seen.get(k, 0), seen_v)

    # 결과 딕셔너리 반환(모델명에 읽기/쓰기 라우팅 표기)
    return meter.report(model=f"couch_{args.read_from}_writeTo{args.write_to}")
