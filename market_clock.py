"""A 股交易时段与行情新鲜度判断。"""
from __future__ import annotations

from datetime import datetime


def in_trading_session(now: datetime, quote_times: list[object] | None = None) -> bool:
    """按时段及行情日期判断市场是否活跃，节假日陈旧行情不会误判。"""
    if now.weekday() >= 5:
        return False
    hm = now.hour * 60 + now.minute
    in_session = ((9 * 60 + 25) <= hm <= (11 * 60 + 35) or
                  (12 * 60 + 55) <= hm <= (15 * 60 + 5))
    if not in_session:
        return False
    valid_times = [value for value in (quote_times or []) if isinstance(value, datetime)]
    return not valid_times or any(value.date() == now.date() for value in valid_times)
