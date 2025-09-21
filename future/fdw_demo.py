import time
from typing import List, Dict, Any

import psycopg  # psycopg3
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

PG_DSN = "host=127.0.0.1 dbname=local_db user=postgres password=postgres port=5432"
MONGO_URI = "mongodb://127.0.0.1:27017"
MONGO_DB = "testdb"
MONGO_COLL = "users"

FDW_SQLS: List[str] = [
    # 1) FDW 확장
    "CREATE EXTENSION IF NOT EXISTS mongo_fdw;",

    # 2) 원격 Mongo 서버 정의 (컨테이너 이름으로 접근)
    """
    CREATE SERVER IF NOT EXISTS mongo_server
    FOREIGN DATA WRAPPER mongo_fdw
    OPTIONS (address 'demo-mongo', port '27017');
    """,

    # 3) 사용자 매핑 (기본 Mongo는 인증 없이도 동작)
    """
    CREATE USER MAPPING IF NOT EXISTS FOR postgres
    SERVER mongo_server
    """,

    # 4) 외래 테이블 매핑 (Mongo: testdb.users -> Postgres: public.mongo_users)
    f"""
    CREATE FOREIGN TABLE IF NOT EXISTS mongo_users (
        _id  TEXT,
        name TEXT,
        age  INT
    )
    SERVER mongo_server
    OPTIONS (database '{MONGO_DB}', collection '{MONGO_COLL}');
    """
]

def wait_for_mongo(timeout_sec: int = 60) -> None:
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=1000)
            client.admin.command("ping")
            client.close()
            return
        except ServerSelectionTimeoutError:
            time.sleep(1)
    raise RuntimeError("Mongo 연결 실패")

def seed_mongo(docs: List[Dict[str, Any]]) -> None:
    client = MongoClient(MONGO_URI)
    coll = client[MONGO_DB][MONGO_COLL]
    # 멱등 시드: 없으면 삽입, 있으면 유지
    for d in docs:
        coll.update_one({"_id": d["_id"]}, {"$setOnInsert": d}, upsert=True)
    client.close()

def wait_for_postgres(timeout_sec: int = 60) -> None:
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            with psycopg.connect(PG_DSN) as conn:
                pass
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("Postgres 연결 실패")

def setup_fdw_and_query() -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            for q in FDW_SQLS:
                cur.execute(q)

            cur.execute("SELECT _id, name, age FROM mongo_users ORDER BY _id;")
            rows = cur.fetchall()

            print("=== FDW로 조회한 Mongo(testdb.users) ===")
            for r in rows:
                print(r)

            cur.execute("SELECT COUNT(*) FROM mongo_users WHERE age >= 28;")
            print("age >= 28 count =", cur.fetchone()[0])

def main():
    wait_for_mongo()
    wait_for_postgres()

    seed_docs = [
        {"_id": 1, "name": "Alice", "age": 25},
        {"_id": 2, "name": "Bob",   "age": 30},
        {"_id": 3, "name": "Carol", "age": 28},
    ]
    seed_mongo(seed_docs)

    setup_fdw_and_query()

if __name__ == "__main__":
    main()