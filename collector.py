"""行情与主力资金流采集（东财轻量接口，行情备用腾讯源）。"""
from __future__ import annotations

import logging
import re
import time
import threading
import urllib.request
from datetime import datetime
from typing import Any

import pandas as pd
import requests

from watchlist import market_of

logger = logging.getLogger(__name__)


def _safe_float(v: Any) -> float | None:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
}

_SESSION_LOCAL = threading.local()


def _session(proxies: dict[str, str]) -> requests.Session:
    """按代理通道复用连接池，减少高频刷新时的握手开销。"""
    sessions = getattr(_SESSION_LOCAL, "sessions", None)
    if sessions is None:
        sessions = {}
        _SESSION_LOCAL.sessions = sessions
    key = tuple(sorted(proxies.items()))
    session = sessions.get(key)
    if session is None:
        session = requests.Session()
        session.trust_env = False
        session.proxies = proxies
        session.headers.update(_HEADERS)
        sessions[key] = session
    return session


def _system_proxies() -> dict[str, str]:
    """环境变量优先，取不到再读 Windows 注册表（IE/系统代理）。"""
    p = urllib.request.getproxies()
    if not (p.get("http") or p.get("https")):
        get_reg = getattr(urllib.request, "getproxies_registry", None)
        if get_reg is not None:
            p = get_reg()
    out: dict[str, str] = {}
    for k in ("http", "https"):
        v = p.get(k)
        if v:
            # 本地代理走 HTTP CONNECT；注册表读出的 https:// 前缀会导致对代理握手 TLS 失败
            out[k] = "http://" + v.split("://", 1)[-1]
    return out


def _get_with_retry(url: str, params: dict | None, tries: int = 2) -> requests.Response:
    """
    行情源偶发断连/限流，且直连与系统代理经常一好一坏：
    每轮先直连、再走系统代理（若有），失败后短暂等待重试。
    """
    channels: list[dict] = [{}]  # 直连
    sys_proxies = _system_proxies()
    if sys_proxies:
        channels.append(sys_proxies)

    last: Exception | None = None
    for i in range(tries):
        for proxies in channels:
            try:
                r = _session(proxies).get(url, params=params, timeout=(4, 10))
                r.raise_for_status()
                return r
            except Exception as e:
                last = e
        if i < tries - 1:
            time.sleep(1 + i)
    raise last  # type: ignore[misc]

def _secid(code: str) -> str:
    """东财 secid：沪市 1.XXXXXX，深市 0.XXXXXX。"""
    return f"{'1' if market_of(code) == 'sh' else '0'}.{code}"


def _fetch_spot_eastmoney(codes: list[str]) -> pd.DataFrame:
    """东财按自选逐只查询（单次请求，避免全市场分页被限流）。"""
    r = _get_with_retry(
        "https://push2.eastmoney.com/api/qt/ulist.np/get",
        params={
            "fltt": 2,
            "invt": 2,
            "fields": "f2,f3,f6,f12,f14,f124",
            "secids": ",".join(_secid(c) for c in codes),
        },
    )
    diff = (r.json().get("data") or {}).get("diff") or []
    if isinstance(diff, dict):
        diff = list(diff.values())
    rows = [
        {
            "code": str(d.get("f12", "")).zfill(6),
            "name": d.get("f14"),
            "price": _safe_float(d.get("f2")),
            "pct": _safe_float(d.get("f3")),
            "amount": _safe_float(d.get("f6")),
            "quote_time": datetime.fromtimestamp(float(d["f124"]))
            if _safe_float(d.get("f124")) else None,
        }
        for d in diff
    ]
    return pd.DataFrame(rows)


def _fetch_spot_tencent(codes: list[str]) -> pd.DataFrame:
    """腾讯行情备用源，GBK 文本协议。"""
    q = ",".join(f"{market_of(c)}{c}" for c in codes)
    r = _get_with_retry(f"https://qt.gtimg.cn/q={q}", params=None)
    text = r.content.decode("gbk", errors="replace")
    rows = []
    for m in re.finditer(r'v_(?:sh|sz)(\d{6})="([^"]*)"', text):
        f = m.group(2).split("~")
        if len(f) < 38 or not f[1]:
            continue
        amount_wan = _safe_float(f[37])  # 成交额单位为万元
        quote_time = None
        try:
            quote_time = datetime.strptime(f[30], "%Y%m%d%H%M%S") if f[30] else None
        except (ValueError, IndexError):
            pass
        rows.append(
            {
                "code": m.group(1),
                "name": f[1],
                "price": _safe_float(f[3]),
                "pct": _safe_float(f[32]),
                "amount": amount_wan * 1e4 if amount_wan is not None else None,
                "quote_time": quote_time,
            }
        )
    return pd.DataFrame(rows)


def fetch_spot_quotes(codes: list[str]) -> tuple[pd.DataFrame, str | None]:
    """
    拉取 A 股实时行情（仅自选股，东财失败时切换腾讯源）。
    返回 (DataFrame, error_message)。失败时 DataFrame 为空且带错误说明。
    """
    if not codes:
        return pd.DataFrame(), "自选股为空"

    errors: list[str] = []
    df = pd.DataFrame()
    source = ""
    for name, fetcher in [("东财", _fetch_spot_eastmoney), ("腾讯", _fetch_spot_tencent)]:
        try:
            df = fetcher(codes)
            if not df.empty:
                source = name
                break
            errors.append(f"{name}返回空数据")
        except Exception as e:
            logger.warning("行情接口失败（%s）：%s", name, e)
            errors.append(f"{name}: {e}")

    if df.empty:
        return pd.DataFrame(), f"行情接口失败（网络/代理/限流问题）：{'；'.join(errors)}"

    # 保持自选顺序
    order = {c: i for i, c in enumerate(codes)}
    out = df[df["code"].isin(order)].copy()
    out["_ord"] = out["code"].map(order)
    out = out.sort_values("_ord").drop(columns=["_ord"])
    fetched_at = datetime.now()
    out["data_source"] = source
    out["fetched_at"] = fetched_at
    if "quote_time" in out.columns:
        out["data_age_sec"] = out["quote_time"].map(
            lambda value: max(0.0, (fetched_at - value).total_seconds())
            if isinstance(value, datetime) else None
        )
    return out.reset_index(drop=True), None


def _fetch_fund_daykline(code: str) -> list[list[str]]:
    """东财个股资金流日线。行格式：日期,主力净额,小单,中单,大单,超大单,主力占比%,..."""
    r = _get_with_retry(
        "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get",
        params={
            "lmt": 0,
            "klt": 101,
            "secid": _secid(code),
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
        },
    )
    klines = (r.json().get("data") or {}).get("klines") or []
    return [str(k).split(",") for k in klines]


def _fund_flow_from_daykline(code: str) -> tuple[dict[str, float | None] | None, str | None]:
    """批量接口失败时的逐只回退：取日线最后一行（当天）。"""
    try:
        rows = _fetch_fund_daykline(code)
    except Exception as e:
        return None, f"{code} 资金流失败: {e}"
    if not rows:
        return None, f"{code} 资金流为空"
    p = rows[-1]
    if len(p) < 7:
        return None, f"{code} 资金流数据异常：{','.join(p)}"
    return {
        "main_net_inflow": _safe_float(p[1]),
        "small_net": _safe_float(p[2]),
        "mid_net": _safe_float(p[3]),
        "big_net": _safe_float(p[4]),
        "super_net": _safe_float(p[5]),
        "main_pct": _safe_float(p[6]),
    }, None


def fetch_fund_flow_history(code: str, days: int = 20) -> list[float | None]:
    """近 N 日主力净流入序列（含今日，按日期升序）。失败返回空列表。"""
    try:
        rows = _fetch_fund_daykline(code)
    except Exception as e:
        logger.warning("资金流历史失败（%s）：%s", code, e)
        return []
    return [_safe_float(p[1]) for p in rows[-days:] if len(p) > 1]


def compute_streak(mains: list[float | None]) -> int:
    """主力连续同向天数：+n 连续净流入 / -n 连续净流出，0 未知。"""
    n = 0
    sign = 0
    for v in reversed(mains):
        if v is None or v == 0:
            break
        s = 1 if v > 0 else -1
        if sign == 0:
            sign = s
        elif s != sign:
            break
        n += 1
    return sign * n


def _fetch_fund_minute(code: str) -> list[list[str]]:
    """东财今日分时资金流（累计值）。行格式：时间,主力,小单,中单,大单,超大单。"""
    r = _get_with_retry(
        "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get",
        params={
            "lmt": 0,
            "klt": 1,
            "secid": _secid(code),
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56",
        },
    )
    klines = (r.json().get("data") or {}).get("klines") or []
    return [str(k).split(",") for k in klines]


def _fund_flow_from_intraday(code: str) -> tuple[dict[str, float | None] | None, str | None]:
    """再退一层：分时累计的最后一行即当前资金流（东财按接口限流，分时常可用）。"""
    try:
        rows = _fetch_fund_minute(code)
    except Exception as e:
        return None, f"{code} 资金流失败: {e}"
    if not rows:
        return None, f"{code} 资金流为空"
    p = rows[-1]
    if len(p) < 6:
        return None, f"{code} 资金流数据异常：{','.join(p)}"
    return {
        "main_net_inflow": _safe_float(p[1]),
        "small_net": _safe_float(p[2]),
        "mid_net": _safe_float(p[3]),
        "big_net": _safe_float(p[4]),
        "super_net": _safe_float(p[5]),
        "main_pct": None,  # 分时接口无占比，enrich 时按成交额折算
    }, None


def compute_burst(intraday_yi: list[float], window_min: int) -> float | None:
    """近 window_min 分钟主力净流入变化（元）。入参为分钟级累计序列（亿）。"""
    if len(intraday_yi) < 2 or window_min < 1:
        return None
    w = min(window_min, len(intraday_yi) - 1)
    return (intraday_yi[-1] - intraday_yi[-1 - w]) * 1e8


def fetch_intraday_main_flow(code: str) -> list[float]:
    """今日分时主力净流入累计序列（亿），用于迷你走势图。失败返回空列表。"""
    try:
        rows = _fetch_fund_minute(code)
    except Exception as e:
        logger.warning("分时资金流失败（%s）：%s", code, e)
        return []
    out: list[float] = []
    for p in rows:
        v = _safe_float(p[1]) if len(p) > 1 else None
        if v is not None:
            out.append(v / 1e8)
    return out


# 今日资金流字段：主力净额/占比 + 超大/大/中/小单净额
_FLOW_FIELDS = {
    "f62": "main_net_inflow",
    "f184": "main_pct",
    "f66": "super_net",
    "f72": "big_net",
    "f78": "mid_net",
    "f84": "small_net",
}
FLOW_COLS = list(_FLOW_FIELDS.values())


def _fetch_fund_flow_bulk(codes: list[str]) -> dict[str, dict[str, float | None]]:
    """东财批量查询今日资金流明细，一次请求覆盖全部自选。"""
    r = _get_with_retry(
        "https://push2.eastmoney.com/api/qt/ulist.np/get",
        params={
            "fltt": 2,
            "invt": 2,
            "fields": "f12," + ",".join(_FLOW_FIELDS),
            "secids": ",".join(_secid(c) for c in codes),
        },
    )
    diff = (r.json().get("data") or {}).get("diff") or []
    if isinstance(diff, dict):
        diff = list(diff.values())
    return {
        str(d.get("f12", "")).zfill(6): {
            col: _safe_float(d.get(f)) for f, col in _FLOW_FIELDS.items()
        }
        for d in diff
    }


def enrich_with_fund_flow(spot: pd.DataFrame) -> pd.DataFrame:
    """给行情表附加今日资金流明细。失败则填空，不中断整表。"""
    if spot.empty:
        return spot

    codes = spot["code"].tolist()
    flow_map: dict[str, dict[str, float | None]] = {}
    try:
        flow_map = _fetch_fund_flow_bulk(codes)
    except Exception as e:
        logger.warning("批量资金流失败，回退逐只查询：%s", e)

    amounts = dict(zip(spot["code"], spot["amount"])) if "amount" in spot.columns else {}
    cols: dict[str, list[float | None]] = {c: [] for c in FLOW_COLS}
    notes: list[str] = []
    for code in codes:
        flow = flow_map.get(code)
        err = None
        if not flow or flow.get("main_net_inflow") is None:
            flow, err = _fund_flow_from_daykline(code)
        if not flow or flow.get("main_net_inflow") is None:
            flow, err = _fund_flow_from_intraday(code)
        flow = flow or {}
        # 占比缺失时按当日成交额折算
        if flow.get("main_pct") is None and flow.get("main_net_inflow") is not None:
            amount = _safe_float(amounts.get(code))
            if amount:
                flow["main_pct"] = flow["main_net_inflow"] / amount * 100
        for c in FLOW_COLS:
            cols[c].append(flow.get(c))
        notes.append(err or "")
    out = spot.copy()
    for c in FLOW_COLS:
        out[c] = cols[c]
    out["fund_flow_note"] = notes
    return out


def format_yi(v: float | None) -> str:
    if v is None:
        return "—"
    return f"{v / 1e8:.2f} 亿"
