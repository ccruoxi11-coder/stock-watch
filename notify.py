"""企业微信群机器人推送。"""
from __future__ import annotations

import logging
from typing import Iterable

import requests

from db import mark_notifications_failed, mark_notifications_sent, pending_notifications
from rules import AlertEvent

logger = logging.getLogger(__name__)


def send_wecom_markdown(webhook: str, content: str) -> tuple[bool, str]:
    if not webhook:
        return False, "未配置 WECOM_WEBHOOK"
    try:
        resp = requests.post(
            webhook,
            json={"msgtype": "markdown", "markdown": {"content": content}},
            timeout=10,
        )
        data = resp.json() if resp.content else {}
        if resp.status_code == 200 and data.get("errcode", 0) == 0:
            return True, "ok"
        return False, f"推送失败: HTTP {resp.status_code} {data}"
    except Exception as e:
        logger.exception("企业微信推送异常")
        return False, str(e)


def format_alerts(events: Iterable[AlertEvent]) -> str:
    lines = ["**A股盯盘告警**", ""]
    for ev in events:
        lines.append(f"> **[{ev.severity}] {ev.code} {ev.name}**")
        lines.append(f"> {ev.reason}")
        lines.append("")
    lines.append("数据来源：东方财富/腾讯（可能延迟/变更，仅供参考）")
    return "\n".join(lines)


def notify_alerts(webhook: str, events: list[AlertEvent]) -> tuple[bool, str]:
    if not events:
        return True, "无新告警"
    return send_wecom_markdown(webhook, format_alerts(events))


def deliver_pending_alerts(webhook: str, limit: int = 50) -> tuple[bool, str, int]:
    """发送到期告警；失败保留并按指数退避重试。"""
    rows = pending_notifications(limit)
    if not rows:
        return True, "无待发送告警", 0
    ids = [int(row["id"]) for row in rows]
    events = [
        AlertEvent(
            code=str(row["code"]),
            name=str(row["name"] or row["code"]),
            reason=str(row["reason"]),
            reason_key="stored",
            pct=row["pct"],
            fund_flow=row["fund_flow"],
            alert_id=int(row["id"]),
            severity=str(row.get("severity") or "重要"),
        )
        for row in rows
    ]
    ok, msg = notify_alerts(webhook, events)
    if ok:
        mark_notifications_sent(ids)
    else:
        mark_notifications_failed(ids, msg)
    return ok, msg, len(ids)
