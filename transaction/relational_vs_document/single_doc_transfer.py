from threading import Lock

class SingleDocumentStore:
    """
    '한 문서(aggregate) 안에 A와 B를 함께 저장'하는 모델.
    - 많은 문서형 DB는 '한 문서' 단위의 원자 업데이트를 보장한다.
    - transform 함수가 끝까지 성공하면 새 문서로 '한 번에 교체'한다.
    """
    def __init__(self, doc: dict):
        # 내부 상태를 복사해 보관
        self.doc = doc.copy()
        # 교체 단위를 보호하기 위한 파이썬 Lock (진짜 DB의 원자성 대체)
        self._lock = Lock()

    def atomic_update(self, transform):
        """
        transform(doc_copy) → new_doc 을 만든 뒤, 락 안에서 '한 번에' 교체.
        transform 도중 예외가 나면 교체하지 않는다(즉, 커밋 자체가 없음).
        """
        with self._lock:
            # 기존 문서를 복사해서 변환에 사용 (실패해도 원본 안전)
            new_doc = transform(self.doc.copy())
            # transform 성공 시점에서만 교체
            self.doc = new_doc

def single_document_transfer(crash_before_commit: bool = False):
    """
    단일 문서로 송금을 모델링:
    - transform 내부에서 A-50, B+50을 모두 적용
    - 커밋 직전에 예외를 발생시키면 교체 자체가 일어나지 않아 원본 유지
    """
    store = SingleDocumentStore({"A": 100, "B": 100})
    try:
        def transform(doc):
            doc["A"] -= 50
            doc["B"] += 50
            if crash_before_commit:
                # 교체 직전 예외 → atomic_update가 교체를 수행하지 않음
                raise RuntimeError("boom before commit")
            return doc
        store.atomic_update(transform)
    except Exception:
        # 예외는 잡지만, 문서 교체는 이뤄지지 않음 → 원본 그대로
        pass
    return store.doc
