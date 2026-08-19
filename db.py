"""SQLite：告警记录、去重与通知投递状态。"""
from __future__ import annotations

import sqlite3
import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).resolve().parent / "data" / "watch.db"


@contextmanager
def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with _conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                name TEXT,
                reason TEXT NOT NULL,
                pct REAL,
                fund_flow REAL,
                created_at TEXT DEFAULT (datetime('now','localtime')),
                notify_status TEXT NOT NULL DEFAULT 'pending',
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                notified_at TEXT,
                next_retry_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS instrument_settings (
                code TEXT PRIMARY KEY,
                group_name TEXT NOT NULL DEFAULT '默认',
                tags TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1,
                price_above REAL,
                price_below REAL,
                short_pct_threshold REAL NOT NULL DEFAULT 0,
                volume_ratio_threshold REAL NOT NULL DEFAULT 0,
                fund_ratio_threshold REAL,
                cooldown_minutes INTEGER NOT NULL DEFAULT 60,
                severity TEXT NOT NULL DEFAULT '重要',
                updated_at TEXT DEFAULT (datetime('now','localtime'))
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quote_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                price REAL,
                pct REAL,
                amount REAL,
                main_net_inflow REAL,
                main_pct REAL,
                captured_at TEXT NOT NULL,
                UNIQUE(code, captured_at)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_quote_history_code_time "
            "ON quote_history(code, captured_at)"
        )
        instrument_columns = {row[1] for row in conn.execute("PRAGMA table_info(instrument_settings)")}
        if "volume_ratio_threshold" not in instrument_columns:
            conn.execute(
                "ALTER TABLE instrument_settings ADD COLUMN volume_ratio_threshold REAL NOT NULL DEFAULT 0"
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quote_snapshot (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                payload TEXT NOT NULL,
                source TEXT,
                quote_count INTEGER NOT NULL DEFAULT 0,
                captured_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS collector_health (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                status TEXT NOT NULL,
                message TEXT,
                pid INTEGER,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                started_at TEXT,
                last_run_at TEXT,
                last_success_at TEXT,
                next_run_at TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now','localtime'))
            )
            """
        )
        # 兼容已有 MVP 数据库，SQLite 不支持一次增加多列，逐列迁移。
        existing = {row[1] for row in conn.execute("PRAGMA table_info(alerts)")}
        added_notify_status = "notify_status" not in existing
        migrations = {
            "notify_status": "TEXT NOT NULL DEFAULT 'pending'",
            "retry_count": "INTEGER NOT NULL DEFAULT 0",
            "last_error": "TEXT",
            "notified_at": "TEXT",
            "next_retry_at": "TEXT",
            "reason_key": "TEXT",
            "severity": "TEXT NOT NULL DEFAULT '重要'",
        }
        for column, definition in migrations.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE alerts ADD COLUMN {column} {definition}")
        # 老告警在升级前已完成原有通知流程，避免升级后被当作待发送告警。
        if added_notify_status:
            conn.execute("UPDATE alerts SET notify_status='skipped'")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_alerts_notify "
            "ON alerts(notify_status, next_retry_at, id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_dedupe (
                code TEXT NOT NULL,
                reason_key TEXT NOT NULL,
                day TEXT NOT NULL,
                PRIMARY KEY (code, reason_key, day)
            )
            """
        )


def save_alert(
    code: str,
    name: str,
    reason: str,
    pct: float | None,
    fund_flow: float | None,
    notify_status: str = "pending",
) -> int:
    with _conn() as conn:
        cursor = conn.execute(
            "INSERT INTO alerts(code, name, reason, pct, fund_flow, notify_status) VALUES (?,?,?,?,?,?)",
            (code, name, reason, pct, fund_flow, notify_status),
        )
        return int(cursor.lastrowid)


def try_claim_alert(code: str, reason_key: str, day: str) -> bool:
    """同一天同一类型告警只推一次。成功插入返回 True。"""
    try:
        with _conn() as conn:
            conn.execute(
                "INSERT INTO alert_dedupe(code, reason_key, day) VALUES (?,?,?)",
                (code, reason_key, day),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def claim_and_save_alert(
    code: str,
    name: str,
    reason: str,
    reason_key: str,
    bucket: str,
    pct: float | None,
    fund_flow: float | None,
    notify_status: str,
    cooldown_minutes: int = 1440,
    severity: str = "重要",
) -> int | None:
    """原子完成冷却检查和事件入库；冷却期内重复事件返回 None。"""
    try:
        with _conn() as conn:
            recent = conn.execute(
                """
                SELECT 1 FROM alerts
                WHERE code=? AND reason_key=?
                  AND created_at >= datetime('now','localtime', ?)
                LIMIT 1
                """,
                (code, reason_key, f"-{max(1, cooldown_minutes)} minutes"),
            ).fetchone()
            if recent:
                return None
            cursor = conn.execute(
                """
                INSERT INTO alerts(
                    code, name, reason, pct, fund_flow, notify_status, reason_key, severity
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (code, name, reason, pct, fund_flow, notify_status, reason_key, severity),
            )
            return int(cursor.lastrowid)
    except sqlite3.IntegrityError:
        return None


def recent_alerts(limit: int = 50) -> list[dict[str, Any]]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def pending_notifications(limit: int = 50) -> list[dict[str, Any]]:
    """返回到达重试时间的待通知告警。"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM alerts
            WHERE notify_status IN ('pending', 'failed')
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY id ASC LIMIT ?
            """,
            (now, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def mark_notifications_sent(alert_ids: list[int]) -> None:
    if not alert_ids:
        return
    placeholders = ",".join("?" for _ in alert_ids)
    with _conn() as conn:
        conn.execute(
            f"UPDATE alerts SET notify_status='sent', notified_at=datetime('now','localtime'), "
            f"last_error=NULL, next_retry_at=NULL WHERE id IN ({placeholders})",
            alert_ids,
        )


def mark_notifications_failed(alert_ids: list[int], error: str) -> None:
    """记录失败并安排指数退避；最长等待 60 分钟。"""
    if not alert_ids:
        return
    with _conn() as conn:
        for alert_id in alert_ids:
            row = conn.execute("SELECT retry_count FROM alerts WHERE id=?", (alert_id,)).fetchone()
            retry_count = int(row[0] if row else 0) + 1
            delay_min = min(2 ** (retry_count - 1), 60)
            next_retry = (datetime.now() + timedelta(minutes=delay_min)).strftime("%Y-%m-%d %H:%M:%S")
            conn.execute(
                """
                UPDATE alerts
                SET notify_status='failed', retry_count=?, last_error=?, next_retry_at=?
                WHERE id=?
                """,
                (retry_count, error[:1000], next_retry, alert_id),
            )


def notification_summary() -> dict[str, int]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT notify_status, COUNT(*) AS count FROM alerts GROUP BY notify_status"
        ).fetchall()
    return {str(r[0]): int(r[1]) for r in rows}


def save_quote_snapshot(payload: str, source: str, quote_count: int, captured_at: str) -> None:
    with _conn() as conn:
        conn.execute(
            """
            INSERT INTO quote_snapshot(id, payload, source, quote_count, captured_at)
            VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                payload=excluded.payload,
                source=excluded.source,
                quote_count=excluded.quote_count,
                captured_at=excluded.captured_at
            """,
            (payload, source, quote_count, captured_at),
        )


def load_quote_snapshot() -> dict[str, Any] | None:
    with _conn() as conn:
        row = conn.execute("SELECT * FROM quote_snapshot WHERE id=1").fetchone()
    return dict(row) if row else None


def update_collector_health(**fields: Any) -> None:
    """局部更新单例采集器状态。"""
    allowed = {
        "status", "message", "pid", "consecutive_failures", "started_at",
        "last_run_at", "last_success_at", "next_run_at",
    }
    values = {key: value for key, value in fields.items() if key in allowed}
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as conn:
        exists = conn.execute("SELECT 1 FROM collector_health WHERE id=1").fetchone()
        if not exists:
            conn.execute(
                "INSERT INTO collector_health(id, status, updated_at) VALUES (1, 'starting', ?)",
                (now,),
            )
        if values:
            assignments = ", ".join(f"{key}=?" for key in values)
            conn.execute(
                f"UPDATE collector_health SET {assignments}, updated_at=? WHERE id=1",
                (*values.values(), now),
            )
        else:
            conn.execute("UPDATE collector_health SET updated_at=? WHERE id=1", (now,))


def get_collector_health() -> dict[str, Any] | None:
    with _conn() as conn:
        row = conn.execute("SELECT * FROM collector_health WHERE id=1").fetchone()
    return dict(row) if row else None


def save_runtime_config(values: dict[str, Any]) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _conn() as conn:
        for key, value in values.items():
            conn.execute(
                """
                INSERT INTO runtime_config(key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (key, json.dumps(value, ensure_ascii=False), now),
            )


def load_runtime_config() -> dict[str, Any]:
    with _conn() as conn:
        rows = conn.execute("SELECT key, value FROM runtime_config").fetchall()
    result: dict[str, Any] = {}
    for row in rows:
        try:
            result[str(row[0])] = json.loads(row[1])
        except (TypeError, json.JSONDecodeError):
            continue
    return result


def upsert_instrument_settings(items: list[dict[str, Any]]) -> None:
    with _conn() as conn:
        for item in items:
            code = str(item.get("code", "")).zfill(6)
            if len(code) != 6 or not code.isdigit():
                continue
            conn.execute(
                """
                INSERT INTO instrument_settings(
                    code, group_name, tags, note, enabled, price_above, price_below,
                    short_pct_threshold, fund_ratio_threshold, cooldown_minutes, severity,
                    volume_ratio_threshold,
                    updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now','localtime'))
                ON CONFLICT(code) DO UPDATE SET
                    group_name=excluded.group_name, tags=excluded.tags, note=excluded.note,
                    enabled=excluded.enabled, price_above=excluded.price_above,
                    price_below=excluded.price_below,
                    short_pct_threshold=excluded.short_pct_threshold,
                    volume_ratio_threshold=excluded.volume_ratio_threshold,
                    fund_ratio_threshold=excluded.fund_ratio_threshold,
                    cooldown_minutes=excluded.cooldown_minutes,
                    severity=excluded.severity, updated_at=excluded.updated_at
                """,
                (
                    code, item.get("group_name") or "默认", item.get("tags") or "",
                    item.get("note") or "", int(bool(item.get("enabled", True))),
                    item.get("price_above"), item.get("price_below"),
                    float(item.get("short_pct_threshold") or 0),
                    item.get("fund_ratio_threshold"),
                    max(1, int(item.get("cooldown_minutes") or 60)),
                    item.get("severity") or "重要",
                    float(item.get("volume_ratio_threshold") or 0),
                ),
            )


def load_instrument_settings(codes: list[str] | None = None) -> list[dict[str, Any]]:
    with _conn() as conn:
        if codes:
            placeholders = ",".join("?" for _ in codes)
            rows = conn.execute(
                f"SELECT * FROM instrument_settings WHERE code IN ({placeholders}) ORDER BY code",
                codes,
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM instrument_settings ORDER BY group_name, code").fetchall()
    return [dict(row) for row in rows]


def save_quote_history(rows: list[dict[str, Any]], captured_at: str) -> None:
    with _conn() as conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO quote_history(
                code, price, pct, amount, main_net_inflow, main_pct, captured_at
            ) VALUES (?,?,?,?,?,?,?)
            """,
            [
                (
                    str(row.get("code", "")), row.get("price"), row.get("pct"),
                    row.get("amount"), row.get("main_net_inflow"), row.get("main_pct"),
                    captured_at,
                )
                for row in rows
            ],
        )


def historical_reference(code: str, minutes: int = 5) -> dict[str, Any] | None:
    with _conn() as conn:
        row = conn.execute(
            """
            SELECT * FROM quote_history
            WHERE code=? AND captured_at <= datetime('now','localtime', ?)
            ORDER BY captured_at DESC LIMIT 1
            """,
            (code, f"-{minutes} minutes"),
        ).fetchone()
    return dict(row) if row else None


def cleanup_history(retention_days: int = 30) -> int:
    with _conn() as conn:
        cursor = conn.execute(
            "DELETE FROM quote_history WHERE captured_at < datetime('now','localtime', ?)",
            (f"-{max(1, retention_days)} days",),
        )
        conn.execute(
            "DELETE FROM alert_dedupe WHERE day < date('now','localtime','-90 days')"
        )
        return int(cursor.rowcount)


def alert_performance(limit: int = 100) -> list[dict[str, Any]]:
    """计算告警后 5/15/30/60 分钟最接近快照的价格表现。"""
    alerts = recent_alerts(limit)
    output: list[dict[str, Any]] = []
    with _conn() as conn:
        for alert in alerts:
            base_price_row = conn.execute(
                "SELECT price FROM quote_history WHERE code=? AND captured_at<=? "
                "AND captured_at>=datetime(?,'-10 minutes') "
                "ORDER BY captured_at DESC LIMIT 1",
                (alert["code"], alert["created_at"], alert["created_at"]),
            ).fetchone()
            base = float(base_price_row[0]) if base_price_row and base_price_row[0] else None
            item = dict(alert)
            for minutes in (5, 15, 30, 60):
                future = conn.execute(
                    """
                    SELECT price FROM quote_history
                    WHERE code=? AND captured_at>=datetime(?, ?)
                      AND captured_at<=datetime(?, ?)
                    ORDER BY captured_at ASC LIMIT 1
                    """,
                    (
                        alert["code"], alert["created_at"], f"+{minutes} minutes",
                        alert["created_at"], f"+{minutes + 15} minutes",
                    ),
                ).fetchone()
                price = float(future[0]) if future and future[0] else None
                item[f"return_{minutes}m"] = ((price / base - 1) * 100) if base and price else None
            output.append(item)
    return output


def backup_database() -> Path:
    backup_dir = DB_PATH.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    target = backup_dir / f"watch-{datetime.now():%Y%m%d-%H%M%S}.db"
    source = sqlite3.connect(DB_PATH)
    destination = sqlite3.connect(target)
    try:
        source.backup(destination)
    finally:
        destination.close()
        source.close()
    backups = sorted(backup_dir.glob("watch-*.db"), key=lambda path: path.stat().st_mtime, reverse=True)
    for old in backups[10:]:
        old.unlink(missing_ok=True)
    return target


def database_stats() -> dict[str, Any]:
    with _conn() as conn:
        tables = {}
        for name in ("alerts", "quote_history", "instrument_settings"):
            tables[name] = int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0])
    return {
        "path": str(DB_PATH),
        "size_mb": round(DB_PATH.stat().st_size / 1024 / 1024, 2) if DB_PATH.exists() else 0,
        **tables,
    }


def recent_quote_history(code: str, limit: int = 240) -> list[dict[str, Any]]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM quote_history WHERE code=? ORDER BY captured_at DESC LIMIT ?",
            (code, limit),
        ).fetchall()
    return [dict(row) for row in reversed(rows)]
