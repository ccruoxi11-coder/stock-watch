"""自选股读写（JSON，便于直接编辑）。"""
from __future__ import annotations

import json
import re
from pathlib import Path

WATCHLIST_PATH = Path(__file__).resolve().parent / "watchlist.json"
_CODE_RE = re.compile(r"^\d{6}$")


def load_watchlist() -> list[str]:
    if not WATCHLIST_PATH.exists():
        return []
    data = json.loads(WATCHLIST_PATH.read_text(encoding="utf-8"))
    codes = []
    for item in data:
        code = str(item).strip()
        if _CODE_RE.match(code):
            codes.append(code)
    # 去重保持顺序
    seen: set[str] = set()
    out: list[str] = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def save_watchlist(codes: list[str]) -> None:
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in codes:
        code = str(item).strip()
        if _CODE_RE.match(code) and code not in seen:
            seen.add(code)
            cleaned.append(code)
    WATCHLIST_PATH.write_text(
        json.dumps(cleaned, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def market_of(code: str) -> str:
    """按代码判断市场：sh / sz / bj。"""
    if code.startswith(("4", "8")):
        return "bj"
    if code.startswith(("5", "6", "9")):
        return "sh"
    return "sz"
