from typing import List, Optional
import json
import os
import time

# ---------- 간단한 영속 로그 ----------
class Log:
    def __init__(self, path: str):
        self.path = path
        # 디렉터리 준비
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # 파일 없으면 생성
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8") as f:
                pass

    def append(self, record: dict) -> None:
        record = dict(record)
        record["ts"] = time.time()
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    def load(self) -> List[dict]:
        out: List[dict] = []
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                out.append(json.loads(line))
        return out

    def last_by_tx(self, txid: str) -> Optional[dict]:
        last = None
        for rec in self.load():
            if rec.get("txid") == txid:
                last = rec
        return last