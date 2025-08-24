import random
import time

from meter import Meter
from pymongo import MongoClient, ReadPreference, WriteConcern
from pymongo.errors import PyMongoError

def run_mongo(args):
    """MongoDB Replica Set에서 리더 기반(ReadPreference, WriteConcern) 조합을 측정"""

    # MongoClient 생성(URI에는 replicaSet=rs0 등을 포함)
    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=8000)
    # 사용할 데이터베이스 핸들
    db = client[args.db]
    # 사용할 컬렉션 핸들
    coll = db[args.coll]
    # 키(k) 고유 인덱스 생성(없으면 생성, 있으면 그대로 통과)
    coll.create_index("k", unique=True)

    # 쓰기 컨센서스(1 또는 majority) 설정
    wc = WriteConcern(w=args.write_concern if args.write_concern != "1" else 1)
    # 쓰기 옵션이 적용된 컬렉션 핸들
    coll_w = coll.with_options(write_concern=wc)

    # 읽기 선호도(primary 또는 secondary) 설정
    rp = ReadPreference.PRIMARY if args.read_from == "primary" else ReadPreference.SECONDARY
    # 읽기 옵션이 적용된 컬렉션 핸들
    coll_r = coll.with_options(read_preference=rp)

    # 메트릭 집계기 생성
    meter = Meter()
    # 재현 가능한 난수 시드(테스트 재실행 시 동작 패턴 비슷하게)
    rng = random.Random(42)

    # 총 연산 수(ops)만큼 루프
    for _ in range(args.ops):
        # 쓰기/읽기 여부를 write_ratio에 따라 결정
        is_write = rng.random() < args.write_ratio
        # 접근할 키를 0..keys-1 범위에서 선택
        k = rng.randrange(args.keys)

        if is_write:
            # 이 키의 현재 버전을 last_written에서 불러오고 +1 증가
            v = meter.last_written.get(k, 0) + 1
            # 지연 측정 시작 시각
            t0 = time.perf_counter()
            try:
                # upsert로 문서 쓰기(없으면 생성, 있으면 v 갱신)
                coll_w.update_one({"k": k}, {"$set": {"v": v, "ts": int(time.time() * 1000)}}, upsert=True)
            except PyMongoError:
                # 에러가 나면 해당 연산은 건너뜀(벤치 계속 진행)
                continue
            # (현재시각 - 시작시각)*1000 → 밀리초 지연 기록
            meter.write_lat.append((time.perf_counter() - t0) * 1000)
            # 쓰기 카운트 증가
            meter.writes += 1
            # 이 키의 마지막 쓴 버전을 기록
            meter.last_written[k] = v
        else:
            # 읽기 지연 측정 시작
            t0 = time.perf_counter()
            # 읽기 수행(프로젝션으로 _id 제외, v만)
            doc = coll_r.find_one({"k": k}, projection={"_id": 0, "v": 1})
            # 읽기 지연 기록
            meter.read_lat.append((time.perf_counter() - t0) * 1000)
            # 읽기 카운트 증가
            meter.reads += 1
            # 읽어서 본 값(문서 없으면 0으로 취급)
            seen_v = doc["v"] if doc and "v" in doc else 0

            # 내가 아는 "마지막 쓴 값"(최신)보다 작으면 stale로 간주
            latest_known = meter.last_written.get(k, 0)
            if seen_v < latest_known:
                meter.stale += 1
                # 최신이 존재하는데 못 봤으면 RYW 위반으로 처리
                if latest_known:
                    meter.ryw_violation += 1
            # 단조 읽기 체크를 위해 last_seen 갱신(여기서는 통계만 사용)
            meter.last_seen[k] = max(meter.last_seen.get(k, 0), seen_v)

    # 결과 딕셔너리 반환(모델명에는 읽기쪽/쓰기 컨선이 드러나게)
    return meter.report(model=f"mongo_{args.read_from}_w{args.write_concern}")