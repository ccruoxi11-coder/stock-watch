"""生成本地只读诊断报告，不包含 Webhook 等敏感配置。"""
from __future__ import annotations

from datetime import datetime
import json
import platform
import sys

from db import database_stats, get_collector_health, init_db, notification_summary
from watchlist import load_watchlist


def build_report() -> dict:
    init_db()
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "python": sys.version,
        "platform": platform.platform(),
        "watchlist_count": len(load_watchlist()),
        "database": database_stats(),
        "collector": get_collector_health(),
        "notifications": notification_summary(),
    }


if __name__ == "__main__":
    print(json.dumps(build_report(), ensure_ascii=False, indent=2))
