"""可由后台进程或页面回退模式调用的一次完整监控循环。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from io import StringIO
import time
from typing import Any

import pandas as pd

from collector import (
    compute_burst,
    compute_streak,
    enrich_with_fund_flow,
    fetch_fund_flow_history,
    fetch_intraday_main_flow,
    fetch_spot_quotes,
)
from db import (
    cleanup_history,
    historical_reference,
    load_instrument_settings,
    save_quote_history,
    save_quote_snapshot,
)
from market_clock import in_trading_session
from notify import deliver_pending_alerts
from rules import evaluate_advanced_row, evaluate_row, filter_new_alerts

_history_cache: dict[str, tuple[float, list[float | None]]] = {}


def _history(code: str, ttl: int = 600) -> list[float | None]:
    cached = _history_cache.get(code)
    now = time.monotonic()
    if cached and now - cached[0] < ttl:
        return cached[1]
    values = fetch_fund_flow_history(code)
    _history_cache[code] = (now, values)
    return values


def _parallel_intraday(codes: list[str]) -> dict[str, list[float]]:
    if not codes:
        return {}
    with ThreadPoolExecutor(max_workers=min(5, len(codes))) as pool:
        values = pool.map(fetch_intraday_main_flow, codes)
    return dict(zip(codes, values))


def dataframe_to_payload(df: pd.DataFrame) -> str:
    return df.to_json(orient="records", force_ascii=False, date_format="iso")


def dataframe_from_payload(payload: str) -> pd.DataFrame:
    df = pd.read_json(StringIO(payload), orient="records", dtype={"code": str})
    for column in ("quote_time", "fetched_at"):
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce").map(
                lambda value: value.to_pydatetime() if not pd.isna(value) else None
            )
    return df


def in_quiet_period(now: datetime, quiet_periods: str) -> bool:
    """判断 HH:MM-HH:MM 格式的逗号分隔静默时段，支持跨午夜。"""
    current = now.hour * 60 + now.minute
    for period in str(quiet_periods or "").split(","):
        try:
            start_text, end_text = period.strip().split("-", 1)
            sh, sm = (int(value) for value in start_text.split(":"))
            eh, em = (int(value) for value in end_text.split(":"))
            start, end = sh * 60 + sm, eh * 60 + em
        except (ValueError, TypeError):
            continue
        if (start <= end and start <= current <= end) or (start > end and (current >= start or current <= end)):
            return True
    return False


def trading_minutes_elapsed(now: datetime) -> int:
    hm = now.hour * 60 + now.minute
    if hm < 9 * 60 + 30:
        return 0
    if hm <= 11 * 60 + 30:
        return hm - (9 * 60 + 30)
    if hm < 13 * 60:
        return 120
    return min(240, 120 + hm - 13 * 60)


def collect_cycle(codes: list[str], settings: dict[str, Any], webhook: str) -> dict[str, Any]:
    """采集、计算规则、持久化快照并投递到期通知。"""
    started = datetime.now()
    spot, error = fetch_spot_quotes(codes)
    if error or spot.empty:
        raise RuntimeError(error or "行情返回空数据")

    fetch_flow = bool(settings.get("fetch_flow", True))
    show_trend = bool(settings.get("show_trend", True))
    if fetch_flow:
        spot = enrich_with_fund_flow(spot)
        spot["main_streak"] = [compute_streak(_history(code)) for code in spot["code"]]
        trading = in_trading_session(started, spot.get("quote_time", pd.Series(dtype=object)).tolist())
        need_intraday = trading and (show_trend or float(settings.get("burst_th", 0)) > 0)
        intraday = _parallel_intraday(spot["code"].tolist()) if need_intraday else {}
        spot["main_burst"] = [
            compute_burst(intraday.get(code, []), int(settings.get("burst_window", 5)))
            if trading else None
            for code in spot["code"]
        ]
        if show_trend:
            spot["intraday_main"] = [intraday.get(code, []) for code in spot["code"]]
    else:
        spot = spot.copy()
        spot["main_net_inflow"] = None
        spot["fund_flow_note"] = ""

    per_code = {item["code"]: item for item in load_instrument_settings(spot["code"].tolist())}
    short_values: list[float | None] = []
    reversals: list[bool] = []
    volume_ratios: list[float | None] = []
    for _, row in spot.iterrows():
        reference = historical_reference(str(row["code"]), 5)
        recent_reference = False
        if reference:
            try:
                age = started - datetime.strptime(str(reference["captured_at"]), "%Y-%m-%d %H:%M:%S")
                recent_reference = timedelta(minutes=4) <= age <= timedelta(minutes=30)
            except (TypeError, ValueError):
                pass
        old_price = float(reference["price"]) if recent_reference and reference.get("price") else None
        price = float(row["price"]) if pd.notna(row.get("price")) else None
        short_values.append((price / old_price - 1) * 100 if price and old_price else None)
        old_flow = float(reference["main_net_inflow"]) if recent_reference and reference.get("main_net_inflow") else None
        flow = float(row["main_net_inflow"]) if pd.notna(row.get("main_net_inflow")) else None
        reversals.append(bool(old_flow and flow and old_flow * flow < 0))
        old_amount = float(reference["amount"]) if recent_reference and reference.get("amount") else None
        amount = float(row["amount"]) if pd.notna(row.get("amount")) else None
        elapsed = trading_minutes_elapsed(started)
        expected_five = amount / elapsed * 5 if amount and elapsed >= 10 else None
        volume_ratios.append(
            max(0.0, amount - old_amount) / expected_five
            if amount and old_amount is not None and expected_five else None
        )
    spot["short_pct"] = short_values
    spot["flow_reversal"] = reversals
    spot["volume_ratio_5m"] = volume_ratios

    events = []
    for _, row in spot.iterrows():
        profile = per_code.get(str(row["code"]), {})
        if profile and not bool(profile.get("enabled", 1)):
            continue
        local_ratio = profile.get("fund_ratio_threshold") if profile else None
        events.extend(
            evaluate_row(
                row.to_dict(),
                float(settings.get("pct_th", 3.0)),
                float(settings.get("fund_th", 5e7)),
                float(local_ratio if local_ratio is not None else settings.get("ratio_th", 10.0)),
                int(settings.get("streak_th", 3)),
                float(settings.get("burst_th", 3e7)),
                int(settings.get("burst_window", 5)),
            )
        )
        events.extend(evaluate_advanced_row(row.to_dict(), profile))
    quiet = in_quiet_period(started, str(settings.get("quiet_periods", "")))
    push_enabled = bool(settings.get("enable_push", False)) and bool(webhook) and not quiet
    fresh = filter_new_alerts(events, notify_enabled=push_enabled)
    push_result = None
    if push_enabled:
        push_result = deliver_pending_alerts(webhook)

    captured = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    history_time = datetime.now().replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    save_quote_history(spot.to_dict(orient="records"), history_time)
    cleanup_history(int(settings.get("history_retention_days", 30)))
    sources = ",".join(sorted(set(spot.get("data_source", pd.Series(dtype=str)).dropna().astype(str))))
    save_quote_snapshot(dataframe_to_payload(spot), sources, len(spot), captured)
    return {
        "dataframe": spot,
        "fresh_alerts": len(fresh),
        "push_result": push_result,
        "captured_at": captured,
        "duration_sec": (datetime.now() - started).total_seconds(),
    }


def worker_interval(now: datetime, refresh_seconds: int) -> int:
    """交易时段高频，盘前/午休 60 秒，其他时间 5 分钟。"""
    hm = now.hour * 60 + now.minute
    weekday = now.weekday() < 5
    if weekday and ((9 * 60 + 15) <= hm <= (11 * 60 + 40) or
                    (12 * 60 + 50) <= hm <= (15 * 60 + 10)):
        return max(10, refresh_seconds)
    if weekday and (8 * 60 + 30) <= hm <= (16 * 60):
        return 60
    return 300
