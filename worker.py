"""独立后台盯盘进程。运行：python worker.py；单次检查：python worker.py --once。"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import os
import signal
import select
import socket
import time
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from config import (
    get_burst_threshold, get_burst_window, get_fund_ratio_threshold,
    get_fund_streak_days, get_fund_threshold, get_pct_threshold,
    get_refresh_seconds, get_webhook,
)
from db import backup_database, init_db, load_runtime_config, update_collector_health
from monitor_service import collect_cycle, worker_interval
from watchlist import load_watchlist

LOCK_PORT = 47651
_running = True
_last_backup_date = None

LOG_DIR = Path(__file__).resolve().parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("stock_watch.worker")
if not logger.handlers:
    handler = RotatingFileHandler(
        LOG_DIR / "worker.log", maxBytes=2 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


def _defaults() -> dict:
    return {
        "pct_th": get_pct_threshold(),
        "fund_th": get_fund_threshold(),
        "ratio_th": get_fund_ratio_threshold(),
        "streak_th": get_fund_streak_days(),
        "burst_th": get_burst_threshold(),
        "burst_window": get_burst_window(),
        "refresh_sec": get_refresh_seconds(),
        "enable_push": bool(get_webhook()),
        "fetch_flow": True,
        "show_trend": True,
        "quiet_periods": "",
        "history_retention_days": 30,
    }


def _settings() -> dict:
    values = _defaults()
    values.update(load_runtime_config())
    return values


def _stop(_signum=None, _frame=None) -> None:
    global _running
    _running = False


def _single_instance_socket() -> socket.socket:
    lock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lock.bind(("127.0.0.1", LOCK_PORT))
    lock.listen(1)
    return lock


def _wait_or_stop(lock: socket.socket, seconds: int) -> None:
    """等待下一轮，同时响应本机停止命令。"""
    global _running
    deadline = time.monotonic() + seconds
    last_heartbeat = time.monotonic()
    while _running and time.monotonic() < deadline:
        timeout = min(1.0, max(0.0, deadline - time.monotonic()))
        readable, _, _ = select.select([lock], [], [], timeout)
        if not readable:
            if time.monotonic() - last_heartbeat >= 30:
                update_collector_health()
                last_heartbeat = time.monotonic()
            continue
        connection, _ = lock.accept()
        with connection:
            command = connection.recv(32).decode("ascii", errors="ignore").strip().upper()
            if command == "STOP":
                connection.sendall(b"OK")
                _running = False


def run(once: bool = False) -> int:
    global _running, _last_backup_date
    init_db()
    lock = None
    if not once:
        try:
            lock = _single_instance_socket()
        except OSError:
            return 2
    started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    update_collector_health(status="starting", message="后台采集器启动中", pid=os.getpid(), started_at=started)
    failures = 0
    try:
        while _running:
            now = datetime.now()
            settings = _settings()
            interval = worker_interval(now, int(settings.get("refresh_sec", 60)))
            next_run = (now + timedelta(seconds=interval)).strftime("%Y-%m-%d %H:%M:%S")
            update_collector_health(status="running", last_run_at=now.strftime("%Y-%m-%d %H:%M:%S"))
            try:
                codes = load_watchlist()
                if not codes:
                    raise RuntimeError("自选股为空")
                result = collect_cycle(codes, settings, get_webhook())
                if _last_backup_date != now.date() and now.hour >= 16:
                    backup = backup_database()
                    _last_backup_date = now.date()
                    logger.info("database backup created: %s", backup)
                failures = 0
                logger.info(
                    "cycle ok codes=%s duration=%.1fs alerts=%s",
                    len(codes), result["duration_sec"], result.get("fresh_alerts", 0),
                )
                update_collector_health(
                    status="healthy",
                    message=f"{len(codes)} 只，耗时 {result['duration_sec']:.1f} 秒",
                    consecutive_failures=0,
                    last_success_at=result["captured_at"],
                    next_run_at=next_run,
                )
            except Exception as exc:
                failures += 1
                logger.exception("collector cycle failed (%s)", failures)
                update_collector_health(
                    status="degraded",
                    message=str(exc)[:500],
                    consecutive_failures=failures,
                    next_run_at=next_run,
                )
            if once:
                return 0 if failures == 0 else 1
            _wait_or_stop(lock, interval)
    finally:
        update_collector_health(status="stopped", message="后台采集器已停止", next_run_at=None)
        if lock:
            lock.close()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    raise SystemExit(run(args.once))
