"""读取环境变量与阈值配置。"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def get_webhook() -> str:
    return os.getenv("WECOM_WEBHOOK", "").strip()


def get_fund_threshold() -> float:
    return float(os.getenv("FUND_FLOW_THRESHOLD", "50000000"))


def get_pct_threshold() -> float:
    return float(os.getenv("PCT_THRESHOLD", "3.0"))


def get_fund_ratio_threshold() -> float:
    return float(os.getenv("FUND_RATIO_THRESHOLD", "10.0"))


def get_fund_streak_days() -> int:
    return int(os.getenv("FUND_STREAK_DAYS", "3"))


def get_burst_threshold() -> float:
    return float(os.getenv("FUND_BURST_THRESHOLD", "30000000"))


def get_burst_window() -> int:
    return int(os.getenv("FUND_BURST_WINDOW", "5"))


def get_refresh_seconds() -> int:
    return int(os.getenv("REFRESH_SECONDS", "60"))
