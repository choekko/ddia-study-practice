import os
import sqlite3

def sqlite_transfer(crash_midway: bool = False):
    """
    SQLite 트랜잭션으로 'A→B 50원 송금'을 시연한다.

    핵심 포인트
    1) BEGIN ~ COMMIT 사이에 예외가 나면 ROLLBACK으로 전체 원복된다.
    2) 'IMMEDIATE'는 쓰기 락을 일찍 잡아 경쟁을 줄인다(가시성 데모용).

    Params
    - crash_midway: True면 A에서 돈 빼고 바로 예외 발생 → 전체 롤백 확인.
    """
    db = "demo_txn.sqlite"
    try:
        os.remove(db)  # 깨끗한 상태로 시작
    except FileNotFoundError:
        pass

    # isolation_level=None → autocommit OFF로 보고 우리가 BEGIN/COMMIT 직접 제어
    conn = sqlite3.connect(db, isolation_level=None)
    cur = conn.cursor()

    # WAL은 동시성/신뢰성 데모에서 흔히 쓰는 저널 모드 (여기선 필수는 아님)
    cur.execute("PRAGMA journal_mode=WAL;")

    # 테이블 준비
    cur.execute("""
      CREATE TABLE accounts(
        id TEXT PRIMARY KEY,
        balance INTEGER NOT NULL
      );
    """)
    cur.execute("INSERT INTO accounts VALUES('A', 100),('B', 100);")

    # 트랜잭션 시작 (IMMEDIATE: 쓰기 락 선점)
    cur.execute("BEGIN IMMEDIATE;")
    try:
        # 1) A에서 50 차감
        cur.execute("UPDATE accounts SET balance = balance - 50 WHERE id = 'A';")

        # 2) 중간 장애 시나리오: 여기서 예외 → COMMIT로 가지 않음
        if crash_midway:
            raise RuntimeError("boom after debit")

        # 3) B에 50 더하기
        cur.execute("UPDATE accounts SET balance = balance + 50 WHERE id = 'B';")

        # 4) 전체 커밋 → 둘 다 반영
        cur.execute("COMMIT;")
    except Exception:
        # 하나라도 실패하면 전체 ROLLBACK → 원자성 보장
        cur.execute("ROLLBACK;")

    # 결과 확인
    cur.execute("SELECT id, balance FROM accounts ORDER BY id;")
    rows = cur.fetchall()

    conn.close()
    os.remove(db)  # 샘플이니 파일은 지움
    return rows  # 예: [('A', 100), ('B', 100)] or [('A', 50), ('B', 150)]
