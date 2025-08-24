import argparse

from run_cassandra import run_cassandra
from run_couch import run_couch
from run_mongo import run_mongo


def main():
    """명령행 인자를 파싱하고 각 모드별 벤치마크 함수를 호출"""
    # 최상위 ArgumentParser 생성(도움말 문구 포함)
    p = argparse.ArgumentParser(description="DDIA Ch.5 실무형 벤치마크 (Mongo/Cassandra/CouchDB)")
    # 서브커맨드 등록을 위한 subparsers 생성
    sub = p.add_subparsers(dest="mode", required=True)

    # ---------------- Mongo 서브커맨드 ----------------
    pm = sub.add_parser("mongo")
    # Mongo 접속 URI(Replica Set 정보 포함)
    pm.add_argument("--mongo-uri", required=True)
    # DB/컬렉션 이름(없으면 생성)
    pm.add_argument("--db", default="bench")
    pm.add_argument("--coll", default="kv")
    # 쓰기 컨선: 1 또는 majority
    pm.add_argument("--write-concern", default="majority", choices=["1", "majority"])
    # 읽기 대상: primary 또는 secondary
    pm.add_argument("--read-from", default="secondary", choices=["primary", "secondary"])
    # 총 연산 수
    pm.add_argument("--ops", type=int, default=5000)
    # 키 공간 크기(충돌/경합에 영향)
    pm.add_argument("--keys", type=int, default=1000)
    # 쓰기 비율(0.0~1.0)
    pm.add_argument("--write-ratio", type=float, default=0.3)

    # -------------- Cassandra 서브커맨드 --------------
    pc = sub.add_parser("cassandra")
    # 접속 호스트(쉼표 구분, 드라이버가 메타 수집 후 다른 노드도 인지)
    pc.add_argument("--hosts", required=True, help="host1,host2,host3  (default port 9042)")
    # 키스페이스/테이블 이름
    pc.add_argument("--keyspace", default="bench")
    pc.add_argument("--table", default="kv")
    # 복제 팩터(RF)
    pc.add_argument("--rf", type=int, default=3)
    # 인증(선택)
    pc.add_argument("--username", default=None)
    pc.add_argument("--password", default=None)
    # ConsistencyLevel(쓰기/읽기)
    pc.add_argument("--write-cl", default="QUORUM", choices=["ONE", "QUORUM", "LOCAL_QUORUM", "ALL"])
    pc.add_argument("--read-cl", default="ONE", choices=["ONE", "QUORUM", "LOCAL_QUORUM", "ALL"])
    # 총 연산 수/키/쓰기 비율
    pc.add_argument("--ops", type=int, default=5000)
    pc.add_argument("--keys", type=int, default=1000)
    pc.add_argument("--write-ratio", type=float, default=0.3)

    # ---------------- CouchDB 서브커맨드 ----------------
    pd = sub.add_parser("couch")
    # A/B 노드 HTTP 베이스(URL, 기본 인증 포함)
    pd.add_argument("--couch-a", required=True, help="ex) http://admin:pass@localhost:5984")
    pd.add_argument("--couch-b", required=True, help="ex) http://admin:pass@localhost:5985")
    # DB 이름
    pd.add_argument("--db", default="bench")
    # 총 연산 수/키/쓰기 비율
    pd.add_argument("--ops", type=int, default=5000)
    pd.add_argument("--keys", type=int, default=1000)
    pd.add_argument("--write-ratio", type=float, default=0.3)
    # 쓰기 라우팅: a/b/both 중 선택
    pd.add_argument("--write-to", default="both", choices=["a", "b", "both"])
    # 읽기 대상: a/b 중 선택
    pd.add_argument("--read-from", default="a", choices=["a", "b"])

    # 인자 파싱 실행
    args = p.parse_args()

    # 서브커맨드에 따라 해당 벤치마크 함수 호출
    if args.mode == "mongo":
        res = run_mongo(args)
    elif args.mode == "cassandra":
        res = run_cassandra(args)
    else:
        res = run_couch(args)

    # 결과를 CSV 한 줄로 표준출력(스크립트/CI에서 파싱하기 쉽게)
    headers = [
        "model",
        "reads",
        "writes",
        "avg_read_ms",
        "p95_read_ms",
        "avg_write_ms",
        "p95_write_ms",
        "stale_read_rate",
        "ryw_violation_rate",
        "conflicts",
    ]
    # 헤더 출력
    print(",".join(headers))
    # 값 출력(키가 없으면 빈 문자열)
    print(",".join(str(res.get(h, "")) for h in headers))

# 스크립트가 직접 실행될 때만 main() 호출(모듈 임포트 시엔 실행 안 함)
if __name__ == "__main__":
    main()