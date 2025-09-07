from dataclasses import dataclass
from typing import Dict

@dataclass
class PrepareReq:
    txid: str
    delta: Dict[str, int]  # account -> delta (+/- 금액)


@dataclass
class PrepareResp:
    txid: str
    vote: str  # "YES" | "NO"


@dataclass
class Decision:
    txid: str
    kind: str  # "COMMIT" | "ABORT"