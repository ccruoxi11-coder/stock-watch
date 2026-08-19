"""告警规则 + 去重。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from db import claim_and_save_alert


@dataclass
class AlertEvent:
    code: str
    name: str
    reason: str
    reason_key: str
    pct: float | None
    fund_flow: float | None
    dedup_bucket: str | None = None  # 默认按天去重；急变类告警按 30 分钟时间桶
    alert_id: int | None = None
    severity: str = "重要"
    cooldown_minutes: int = 1440


def _half_hour_bucket() -> str:
    now = datetime.now()
    return f"{now:%Y-%m-%d %H}:{'00' if now.minute < 30 else '30'}"


def _to_float(v: Any) -> float | None:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def evaluate_row(
    row: dict[str, Any],
    pct_threshold: float,
    fund_threshold: float,
    ratio_threshold: float = 0.0,
    streak_threshold: int = 0,
    burst_threshold: float = 0.0,
    burst_window: int = 5,
) -> list[AlertEvent]:
    events: list[AlertEvent] = []
    code = str(row.get("code", ""))
    name = str(row.get("name") or code)

    pct_f = _to_float(row.get("pct"))
    flow_f = _to_float(row.get("main_net_inflow"))
    ratio_f = _to_float(row.get("main_pct"))
    streak_f = _to_float(row.get("main_streak"))
    streak = int(streak_f) if streak_f is not None and streak_f == streak_f else 0  # NaN 安全

    if pct_f is not None and abs(pct_f) > pct_threshold:
        events.append(
            AlertEvent(
                code=code,
                name=name,
                reason=f"涨跌幅 {pct_f:.2f}% ，超过 ±{pct_threshold}%",
                reason_key="pct",
                pct=pct_f,
                fund_flow=flow_f,
                severity="重要",
            )
        )

    if flow_f is not None and fund_threshold > 0 and abs(flow_f) >= fund_threshold:
        direction = "净流入" if flow_f >= 0 else "净流出"
        events.append(
            AlertEvent(
                code=code,
                name=name,
                reason=f"主力{direction} {flow_f/1e8:.2f} 亿，达到阈值 {fund_threshold/1e8:.2f} 亿",
                reason_key="fund",
                pct=pct_f,
                fund_flow=flow_f,
                severity="重要",
            )
        )

    if ratio_f is not None and ratio_threshold > 0 and abs(ratio_f) >= ratio_threshold:
        direction = "净流入" if ratio_f >= 0 else "净流出"
        events.append(
            AlertEvent(
                code=code,
                name=name,
                reason=f"主力{direction}占成交额 {abs(ratio_f):.1f}%，达到阈值 {ratio_threshold:.0f}%",
                reason_key="fund_ratio",
                pct=pct_f,
                fund_flow=flow_f,
                severity="重要",
            )
        )

    burst_f = _to_float(row.get("main_burst"))
    if burst_f is not None and burst_f == burst_f and burst_threshold > 0 and abs(burst_f) >= burst_threshold:
        direction = "流入" if burst_f >= 0 else "流出"
        events.append(
            AlertEvent(
                code=code,
                name=name,
                reason=f"盘中急变：近 {burst_window} 分钟主力净{direction} {abs(burst_f)/1e8:.2f} 亿",
                reason_key="fund_burst",
                pct=pct_f,
                fund_flow=flow_f,
                dedup_bucket=_half_hour_bucket(),
                severity="紧急",
                cooldown_minutes=30,
            )
        )

    if streak != 0 and streak_threshold > 0 and abs(streak) >= streak_threshold:
        direction = "净流入" if streak > 0 else "净流出"
        today = f"，今日 {flow_f/1e8:.2f} 亿" if flow_f is not None else ""
        events.append(
            AlertEvent(
                code=code,
                name=name,
                reason=f"主力连续 {abs(streak)} 日{direction}{today}",
                reason_key="fund_streak",
                pct=pct_f,
                fund_flow=flow_f,
                severity="提示",
            )
        )
    return events


def evaluate_advanced_row(row: dict[str, Any], settings: dict[str, Any]) -> list[AlertEvent]:
    """计算单股价格、短时异动、资金反转和组合规则。"""
    if not bool(settings.get("enabled", True)):
        return []
    code = str(row.get("code", ""))
    name = str(row.get("name") or code)
    price = _to_float(row.get("price"))
    pct = _to_float(row.get("pct"))
    flow = _to_float(row.get("main_net_inflow"))
    ratio = _to_float(row.get("main_pct"))
    short_pct = _to_float(row.get("short_pct"))
    flow_reversal = bool(row.get("flow_reversal", False))
    volume_ratio = _to_float(row.get("volume_ratio_5m"))
    severity = str(settings.get("severity") or "重要")
    cooldown = max(1, int(settings.get("cooldown_minutes") or 60))
    events: list[AlertEvent] = []

    above = _to_float(settings.get("price_above"))
    below = _to_float(settings.get("price_below"))
    if price is not None and above is not None and above > 0 and price >= above:
        events.append(AlertEvent(code, name, f"价格 {price:.2f} 已突破 {above:.2f}", "price_above", pct, flow,
                                 severity=severity, cooldown_minutes=cooldown))
    if price is not None and below is not None and below > 0 and price <= below:
        events.append(AlertEvent(code, name, f"价格 {price:.2f} 已跌破 {below:.2f}", "price_below", pct, flow,
                                 severity="紧急", cooldown_minutes=cooldown))

    short_threshold = float(settings.get("short_pct_threshold") or 0)
    short_hit = short_pct is not None and short_threshold > 0 and abs(short_pct) >= short_threshold
    if short_hit:
        direction = "拉升" if short_pct >= 0 else "下跌"
        events.append(AlertEvent(code, name, f"5 分钟快速{direction} {abs(short_pct):.2f}%", "short_pct", pct, flow,
                                 severity=severity, cooldown_minutes=cooldown))

    if flow_reversal:
        direction = "流入" if flow is not None and flow >= 0 else "流出"
        events.append(AlertEvent(code, name, f"主力资金方向反转为净{direction}", "flow_reversal", pct, flow,
                                 severity="提示", cooldown_minutes=cooldown))

    volume_threshold = float(settings.get("volume_ratio_threshold") or 0)
    volume_hit = volume_ratio is not None and volume_threshold > 0 and volume_ratio >= volume_threshold
    if volume_hit:
        events.append(AlertEvent(
            code, name, f"近 5 分钟成交额速度达到日内均速 {volume_ratio:.1f} 倍",
            "volume_burst", pct, flow, severity=severity, cooldown_minutes=cooldown,
        ))

    ratio_threshold = _to_float(settings.get("fund_ratio_threshold"))
    ratio_hit = ratio is not None and ratio_threshold is not None and ratio_threshold > 0 and abs(ratio) >= ratio_threshold
    if short_hit and ratio_hit and short_pct is not None and ratio is not None and short_pct * ratio > 0:
        direction = "向上" if short_pct > 0 else "向下"
        events.append(AlertEvent(
            code, name,
            f"组合异动：5 分钟{direction} {abs(short_pct):.2f}%，主力占比 {ratio:.1f}%",
            "composite_momentum", pct, flow, severity="紧急", cooldown_minutes=cooldown,
        ))
    if short_hit and volume_hit and short_pct is not None:
        direction = "拉升" if short_pct > 0 else "下跌"
        events.append(AlertEvent(
            code, name,
            f"量价异动：5 分钟{direction} {abs(short_pct):.2f}%，成交速度 {volume_ratio:.1f} 倍",
            "composite_price_volume", pct, flow, severity="紧急", cooldown_minutes=cooldown,
        ))
    return events


def filter_new_alerts(events: list[AlertEvent], notify_enabled: bool = True) -> list[AlertEvent]:
    """按事件冷却时间过滤重复告警，并原子写入数据库。"""
    day = date.today().isoformat()
    fresh: list[AlertEvent] = []
    for ev in events:
        ev.alert_id = claim_and_save_alert(
            ev.code,
            ev.name,
            ev.reason,
            ev.reason_key,
            ev.dedup_bucket or day,
            ev.pct,
            ev.fund_flow,
            "pending" if notify_enabled else "skipped",
            ev.cooldown_minutes,
            ev.severity,
        )
        if ev.alert_id is not None:
            fresh.append(ev)
    return fresh
