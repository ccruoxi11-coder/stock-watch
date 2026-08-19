"""
A股盯盘 + 资金流监控（MVP）
运行：streamlit run app.py
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

from collector import (
    compute_burst,
    compute_streak,
    enrich_with_fund_flow,
    fetch_fund_flow_history,
    fetch_intraday_main_flow,
    fetch_spot_quotes,
)
from config import (
    get_burst_threshold,
    get_burst_window,
    get_fund_ratio_threshold,
    get_fund_streak_days,
    get_fund_threshold,
    get_pct_threshold,
    get_refresh_seconds,
    get_webhook,
)
from db import (
    alert_performance,
    backup_database,
    cleanup_history,
    database_stats,
    get_collector_health,
    init_db,
    load_instrument_settings,
    load_quote_snapshot,
    notification_summary,
    recent_alerts,
    recent_quote_history,
    save_runtime_config,
    upsert_instrument_settings,
)
from market_clock import in_trading_session
from monitor_service import collect_cycle, dataframe_from_payload
from notify import deliver_pending_alerts, send_wecom_markdown
from rules import evaluate_row, filter_new_alerts
from watchlist import load_watchlist, save_watchlist

st.set_page_config(page_title="A股盯盘资金流", page_icon="📈", layout="wide")
init_db()

st.title("A股盯盘 · 资金流监控")
st.caption(
    "数据源：东方财富/腾讯（免费接口，字段/可用性可能变更；行情可能有延迟，仅供参考，不构成投资建议）"
)

with st.sidebar:
    st.header("自选股")
    codes0 = load_watchlist()
    text = st.text_area(
        "每行一个 6 位代码",
        value="\n".join(codes0),
        height=180,
        help="编辑后点保存",
    )
    if st.button("保存自选股", type="primary"):
        new_codes = [line.strip() for line in text.splitlines() if line.strip()]
        save_watchlist(new_codes)
        st.success(f"已保存 {len(load_watchlist())} 只")
        st.rerun()

    st.divider()
    st.header("告警阈值")
    pct_th = st.number_input("|涨跌幅| > %", value=float(get_pct_threshold()), min_value=0.1, step=0.5)
    fund_th_yi = st.number_input(
        "主力净流入绝对值 ≥（亿，0=关闭）",
        value=float(get_fund_threshold()) / 1e8,
        min_value=0.0,
        step=0.5,
    )
    fund_th = fund_th_yi * 1e8
    ratio_th = st.number_input(
        "主力净流入占成交额 ≥ %（0=关闭）",
        value=float(get_fund_ratio_threshold()),
        min_value=0.0,
        step=1.0,
        help="主力净流入(出)金额占当日成交额的比例，小盘股建议 10% 以上",
    )
    streak_th = st.number_input(
        "主力连续同向 ≥ 天（0=关闭）",
        value=int(get_fund_streak_days()),
        min_value=0,
        step=1,
        help="主力资金连续 N 个交易日净流入或净流出",
    )
    burst_window = st.number_input(
        "盘中急变窗口（分钟）", value=int(get_burst_window()), min_value=1, max_value=30, step=1
    )
    burst_th_yi = st.number_input(
        "盘中急变阈值（亿，0=关闭）",
        value=float(get_burst_threshold()) / 1e8,
        min_value=0.0,
        step=0.1,
        help="近 N 分钟主力净流入/流出达到该金额即告警；同一股票 30 分钟内不重复，仅交易时段生效",
    )
    burst_th = burst_th_yi * 1e8
    refresh_sec = st.number_input(
        "刷新间隔（秒）", value=int(get_refresh_seconds()), min_value=10, step=5
    )
    auto_refresh = st.checkbox("自动刷新", value=True)
    enable_push = st.checkbox("启用企业微信推送", value=bool(get_webhook()))
    webhook = get_webhook()
    if enable_push and not webhook:
        st.warning("未配置 .env 里的 WECOM_WEBHOOK，请复制 .env.example 为 .env 后填写")
    fetch_flow = st.checkbox("拉取主力资金流明细", value=True)
    show_trend = st.checkbox("显示分时主力趋势图", value=True)
    quiet_periods = st.text_input(
        "通知静默时段", value="", placeholder="例如 11:30-13:00,22:00-08:00"
    )
    history_retention_days = st.number_input("分钟历史保留天数", 1, 365, 30)

save_runtime_config(
    {
        "pct_th": pct_th,
        "fund_th": fund_th,
        "ratio_th": ratio_th,
        "streak_th": int(streak_th),
        "burst_th": burst_th,
        "burst_window": int(burst_window),
        "refresh_sec": int(refresh_sec),
        "enable_push": enable_push,
        "fetch_flow": fetch_flow,
        "show_trend": show_trend,
        "quiet_periods": quiet_periods,
        "history_retention_days": int(history_retention_days),
    }
)


@st.cache_data(ttl=600, show_spinner=False)
def cached_flow_history(code: str) -> list[float | None]:
    """资金流日线只随交易日变化，缓存 10 分钟减少请求量。"""
    return fetch_fund_flow_history(code)


@st.cache_data(ttl=55, show_spinner=False)
def cached_intraday(code: str) -> list[float]:
    return fetch_intraday_main_flow(code)


def load_and_alert(
    codes: list[str],
    *,
    fetch_flow: bool,
    show_trend: bool,
    pct_th: float,
    fund_th: float,
    ratio_th: float,
    streak_th: int,
    burst_th: float,
    burst_window: int,
    enable_push: bool,
    webhook: str,
) -> tuple[pd.DataFrame, str | None, list[str], tuple[str, str] | None]:
    spot, err = fetch_spot_quotes(codes)
    flash: list[str] = []
    push_info: tuple[str, str] | None = None
    if err or spot.empty:
        return spot, err, flash, push_info

    if fetch_flow:
        spot = enrich_with_fund_flow(spot)
        spot["main_streak"] = [
            compute_streak(cached_flow_history(c)) for c in spot["code"]
        ]
        intraday_map = {c: cached_intraday(c) for c in spot["code"]}
        trading = in_trading_session(datetime.now(), spot.get("quote_time", pd.Series()).tolist())
        spot["main_burst"] = [
            compute_burst(intraday_map[c], burst_window) if trading else None
            for c in spot["code"]
        ]
        if show_trend:
            spot["intraday_main"] = [intraday_map[c] for c in spot["code"]]
    else:
        spot = spot.copy()
        spot["main_net_inflow"] = None
        spot["fund_flow_note"] = ""

    events = []
    for _, row in spot.iterrows():
        events.extend(
            evaluate_row(
                row.to_dict(), pct_th, fund_th, ratio_th, streak_th, burst_th, burst_window
            )
        )
    fresh = filter_new_alerts(events, notify_enabled=enable_push and bool(webhook))
    if fresh:
        flash = [f"{e.code} {e.name}: {e.reason}" for e in fresh]
    if enable_push and webhook:
        ok, msg, count = deliver_pending_alerts(webhook)
        if count:
            push_info = ("success" if ok else "error", f"{msg}（{count} 条）")
    return spot, None, flash, push_info


def _to_yi(v: object) -> float | None:
    try:
        f = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return None if f != f else f / 1e8


def render_table(df: pd.DataFrame, codes: list[str]) -> None:
    show = pd.DataFrame()
    show["代码"] = df["code"]
    if "name" in df.columns:
        show["名称"] = df["name"]
    if "price" in df.columns:
        show["最新价"] = df["price"]
    if "pct" in df.columns:
        show["涨跌幅%"] = df["pct"]
    if "amount" in df.columns:
        show["成交额(亿)"] = df["amount"].map(_to_yi)
    for col, label in [
        ("main_net_inflow", "主力净流入(亿)"),
        ("super_net", "超大单(亿)"),
        ("big_net", "大单(亿)"),
        ("mid_net", "中单(亿)"),
        ("small_net", "小单(亿)"),
    ]:
        if col in df.columns:
            show[label] = df[col].map(_to_yi)
    if "main_pct" in df.columns:
        show["主力占比%"] = df["main_pct"]
    if "main_burst" in df.columns:
        show["盘中急变(亿)"] = df["main_burst"].map(_to_yi)
    if "main_streak" in df.columns:
        show["主力连续"] = df["main_streak"].map(
            lambda s: "—" if not s or s != s else f"{'流入' if s > 0 else '流出'} {abs(int(s))} 天"
        )
    if "data_source" in df.columns:
        show["数据源"] = df["data_source"]
    if "quote_time" in df.columns:
        show["行情时间"] = df["quote_time"].map(
            lambda value: value.strftime("%m-%d %H:%M:%S") if isinstance(value, datetime) else "—"
        )
    if "data_age_sec" in df.columns:
        show["数据状态"] = df["data_age_sec"].map(
            lambda age: "未知" if age is None or pd.isna(age) else ("实时" if age <= 180 else "陈旧")
        )
    if "intraday_main" in df.columns:
        show["分时主力(亿)"] = df["intraday_main"]

    column_config: dict = {
        "最新价": st.column_config.NumberColumn(format="%.2f"),
        "涨跌幅%": st.column_config.NumberColumn(format="%.2f"),
        "成交额(亿)": st.column_config.NumberColumn(format="%.2f"),
        "主力净流入(亿)": st.column_config.NumberColumn(format="%.2f"),
        "超大单(亿)": st.column_config.NumberColumn(format="%.2f"),
        "大单(亿)": st.column_config.NumberColumn(format="%.2f"),
        "中单(亿)": st.column_config.NumberColumn(format="%.2f"),
        "小单(亿)": st.column_config.NumberColumn(format="%.2f"),
        "主力占比%": st.column_config.NumberColumn(format="%.2f"),
        "盘中急变(亿)": st.column_config.NumberColumn(
            format="%.2f", help="近 N 分钟主力净流入变化，仅交易时段计算"
        ),
        "分时主力(亿)": st.column_config.LineChartColumn(width="medium"),
    }
    st.dataframe(show, width="stretch", hide_index=True, column_config=column_config)
    missing = [c for c in codes if c not in set(df["code"].astype(str))]
    if missing:
        st.info(f"未找到行情的代码：{', '.join(missing)}")


codes = load_watchlist()
if not codes:
    st.info("请先在左侧添加自选股代码并保存。")
    st.stop()

manual = st.button("立即刷新", type="primary")


def active_worker() -> tuple[bool, dict | None]:
    health = get_collector_health()
    if not health or health.get("status") == "stopped":
        return False, health
    try:
        updated = datetime.strptime(str(health["updated_at"]), "%Y-%m-%d %H:%M:%S")
    except (KeyError, TypeError, ValueError):
        return False, health
    alive = (datetime.now() - updated).total_seconds() <= max(180, int(refresh_sec) * 3)
    return alive, health


def run_panel() -> None:
    worker_alive, health = active_worker()
    if worker_alive:
        status = str(health.get("status", "unknown")) if health else "unknown"
        message = str(health.get("message") or "") if health else ""
        icon = "🟢" if status == "healthy" else "🟡"
        st.caption(
            f"{icon} 后台采集器：{status}｜{message}｜"
            f"最近成功：{health.get('last_success_at') or '等待首次采集'}｜"
            f"下次采集：{health.get('next_run_at') or '计算中'}"
        )
        snapshot = load_quote_snapshot()
        if not snapshot:
            st.info("后台采集器正在准备第一份行情快照，请稍候。")
            return
        try:
            df = dataframe_from_payload(str(snapshot["payload"]))
        except Exception as exc:
            st.error(f"读取后台行情快照失败：{exc}")
            return
        if "code" in df.columns:
            df["code"] = df["code"].astype(str).str.zfill(6)
            df = df[df["code"].isin(codes)].copy()
        st.write(f"行情快照：{snapshot['captured_at']}（页面只读本地数据库）")
        if df.empty:
            st.warning("当前快照中没有自选股数据，后台将在下一轮同步。")
        else:
            render_table(df, codes)
        return

    if health:
        st.warning("后台采集器心跳已中断，当前自动切换为页面直连采集。")
    else:
        st.caption("后台采集器未启动，当前使用页面直连兼容模式。")
    st.write(f"刷新时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    settings = {
        "fetch_flow": fetch_flow, "show_trend": show_trend, "pct_th": pct_th,
        "fund_th": fund_th, "ratio_th": ratio_th, "streak_th": int(streak_th),
        "burst_th": burst_th, "burst_window": int(burst_window),
        "enable_push": enable_push, "quiet_periods": quiet_periods,
        "history_retention_days": int(history_retention_days),
    }
    with st.spinner("拉取行情 / 资金流…"):
        try:
            result = collect_cycle(codes, settings, webhook)
            df = result["dataframe"]
        except Exception as exc:
            st.error(f"采集失败：{exc}")
            return
    render_table(df, codes)


# Streamlit 1.33+ 支持 fragment 定时刷新；否则仅手动刷新
_use_fragment = hasattr(st, "fragment") and auto_refresh

if _use_fragment:

    @st.fragment(run_every=timedelta(seconds=int(refresh_sec)))
    def live() -> None:
        run_panel()

    if manual:
        st.rerun()
    live()
else:
    if auto_refresh:
        st.info("当前 Streamlit 版本不支持自动 fragment 刷新，请升级 streamlit>=1.33，或使用「立即刷新」。")
    run_panel()

st.subheader("最近告警")
notify_stats = notification_summary()
if notify_stats.get("failed", 0) or notify_stats.get("pending", 0):
    st.caption(
        f"通知队列：待发送 {notify_stats.get('pending', 0)}，"
        f"等待重试 {notify_stats.get('failed', 0)}，已发送 {notify_stats.get('sent', 0)}"
    )
alerts = recent_alerts(30)
if alerts:
    st.dataframe(pd.DataFrame(alerts), width="stretch", hide_index=True)
else:
    st.caption("暂无告警记录（重复提醒由全局或单股冷却时间控制）")

st.divider()
settings_tab, detail_tab, review_tab, system_tab = st.tabs(
    ["单股规则", "个股详情", "告警复盘", "系统维护"]
)

with settings_tab:
    st.caption("每只股票可以独立设置分组、价格线、5分钟异动、资金占比、等级和冷却时间。0或留空表示关闭。")
    stored = {item["code"]: item for item in load_instrument_settings(codes)}
    rows = []
    for code in codes:
        item = stored.get(code, {})
        rows.append({
            "code": code,
            "group_name": item.get("group_name", "默认"),
            "tags": item.get("tags", ""),
            "note": item.get("note", ""),
            "enabled": bool(item.get("enabled", 1)),
            "price_above": item.get("price_above"),
            "price_below": item.get("price_below"),
            "short_pct_threshold": item.get("short_pct_threshold", 0.0),
            "volume_ratio_threshold": item.get("volume_ratio_threshold", 0.0),
            "fund_ratio_threshold": item.get("fund_ratio_threshold"),
            "cooldown_minutes": item.get("cooldown_minutes", 60),
            "severity": item.get("severity", "重要"),
        })
    edited = st.data_editor(
        pd.DataFrame(rows), hide_index=True, width="stretch", num_rows="fixed",
        disabled=["code"],
        column_config={
            "code": "代码", "group_name": "分组", "tags": "标签", "note": "备注",
            "enabled": "启用", "price_above": "突破价", "price_below": "跌破价",
            "short_pct_threshold": "5分钟异动%", "fund_ratio_threshold": "主力占比%",
            "volume_ratio_threshold": "成交速度倍数",
            "cooldown_minutes": "冷却分钟",
            "severity": st.column_config.SelectboxColumn("等级", options=["提示", "重要", "紧急"]),
        },
    )
    if st.button("保存单股规则", type="primary"):
        upsert_instrument_settings(edited.where(pd.notna(edited), None).to_dict(orient="records"))
        st.success("单股规则已保存，后台下一轮采集生效。")

with detail_tab:
    selected_code = st.selectbox("选择股票", codes)
    detail_rows = recent_quote_history(selected_code, 240)
    selected_setting = stored.get(selected_code, {}) if codes else {}
    if selected_setting.get("note"):
        st.info(f"备注：{selected_setting['note']}")
    if detail_rows:
        detail_df = pd.DataFrame(detail_rows)
        detail_df["captured_at"] = pd.to_datetime(detail_df["captured_at"])
        st.line_chart(detail_df.set_index("captured_at")[["price"]])
        st.dataframe(detail_df.tail(30), width="stretch", hide_index=True)
    else:
        st.caption("尚无分钟历史，后台运行后会自动积累。")

with review_tab:
    performance = alert_performance(100)
    if performance:
        performance_df = pd.DataFrame(performance)
        columns = [
            col for col in ["created_at", "severity", "code", "name", "reason",
                            "return_5m", "return_15m", "return_30m", "return_60m"]
            if col in performance_df.columns
        ]
        st.dataframe(performance_df[columns], width="stretch", hide_index=True)
    else:
        st.caption("历史样本不足，产生告警并积累后续行情后会显示收益表现。")

with system_tab:
    stats = database_stats()
    c1, c2, c3 = st.columns(3)
    c1.metric("数据库大小", f"{stats['size_mb']} MB")
    c2.metric("分钟快照", stats["quote_history"])
    c3.metric("告警总数", stats["alerts"])
    st.json(get_collector_health() or {"status": "未启动"})
    if st.button("立即备份数据库"):
        st.success(f"备份完成：{backup_database()}")
    if st.button("清理过期分钟数据"):
        deleted = cleanup_history(int(history_retention_days))
        st.success(f"已清理 {deleted} 条过期记录。")
    if st.button("测试企业微信通知"):
        ok, msg = send_wecom_markdown(webhook, "**A股盯盘测试**\n\n> 通知通道工作正常")
        (st.success if ok else st.error)(msg)
