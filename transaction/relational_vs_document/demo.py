from .sqlite_transfer import sqlite_transfer
from .naive_kv_transfer import naive_kv_transfer
from .single_doc_transfer import single_document_transfer

def run():
    print("=== 관계형 vs 문서형 모델에서의 트랜잭션 ===")
    print(" A, B 각각 잔고 100원이 있을 때, A → B 50원 송금 을 시연\n")

    print("[SQLite: 중간 에러 → 전체 ROLLBACK]")
    print(" ->", sqlite_transfer(crash_midway=True))

    print("[트랜잭션이 없는 KV: 중간 에러 → 절반만 반영]")
    print(" ->", naive_kv_transfer(crash_midway=True))

    print("[단일 문서 원자 교체: 커밋 전에 예외 → 원본 유지]")
    print(" ->", single_document_transfer(crash_before_commit=True))

if __name__ == "__main__":
    run()