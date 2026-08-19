"""验证行情/资金流接口是否可用：python test_data.py"""
from __future__ import annotations

from collector import (
    compute_burst,
    compute_streak,
    enrich_with_fund_flow,
    fetch_fund_flow_history,
    fetch_intraday_main_flow,
    fetch_spot_quotes,
    format_yi,
)
from watchlist import load_watchlist


def main() -> None:
    codes = load_watchlist() or ["600519", "000001", "300750"]
    print("自选:", codes)
    print("--- 行情 ---")
    df, err = fetch_spot_quotes(codes)
    if err:
        print("ERROR:", err)
        return
    print(df.to_string(index=False))
    print("--- 今日资金流明细 ---")
    df = enrich_with_fund_flow(df)
    for _, row in df.iterrows():
        pct = row.get("main_pct")
        print(
            row["code"],
            "主力", format_yi(row["main_net_inflow"]),
            "占比", f"{pct:.2f}%" if pct is not None else "—",
            "超大单", format_yi(row.get("super_net")),
            "大单", format_yi(row.get("big_net")),
            row["fund_flow_note"] or "ok",
        )
    print("--- 连续性 / 分时 / 急变 ---")
    for code in codes:
        streak = compute_streak(fetch_fund_flow_history(code))
        trend = fetch_intraday_main_flow(code)
        burst = compute_burst(trend, 5)
        streak_s = "—" if streak == 0 else f"{'流入' if streak > 0 else '流出'}{abs(streak)}天"
        print(
            code,
            "主力连续:", streak_s,
            "| 分时点数:", len(trend),
            "| 近5分钟主力:", format_yi(burst),
        )


if __name__ == "__main__":
    main()
