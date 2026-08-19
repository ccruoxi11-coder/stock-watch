from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import db
from market_clock import in_trading_session
from monitor_service import (
    dataframe_from_payload, dataframe_to_payload, in_quiet_period, worker_interval,
)
from notify import deliver_pending_alerts
from rules import AlertEvent, evaluate_advanced_row, evaluate_row, filter_new_alerts
from watchlist import market_of
import pandas as pd
import worker


class RuleTests(unittest.TestCase):
    def test_zero_fund_threshold_disables_rule(self) -> None:
        events = evaluate_row(
            {"code": "600000", "name": "测试", "pct": 0, "main_net_inflow": 1},
            pct_threshold=3,
            fund_threshold=0,
        )
        self.assertNotIn("fund", [event.reason_key for event in events])

    def test_advanced_composite_rules(self) -> None:
        events = evaluate_advanced_row(
            {
                "code": "600000", "name": "测试", "price": 11, "pct": 2,
                "main_net_inflow": 1e8, "main_pct": 12, "short_pct": 2.5,
                "volume_ratio_5m": 3.0,
            },
            {
                "enabled": True, "price_above": 10, "short_pct_threshold": 2,
                "fund_ratio_threshold": 10, "volume_ratio_threshold": 2,
                "cooldown_minutes": 15,
            },
        )
        keys = {event.reason_key for event in events}
        self.assertTrue({"price_above", "composite_momentum", "composite_price_volume"} <= keys)

    def test_market_mapping_supports_beijing(self) -> None:
        self.assertEqual(market_of("830001"), "bj")
        self.assertEqual(market_of("600000"), "sh")
        self.assertEqual(market_of("000001"), "sz")


class MarketClockTests(unittest.TestCase):
    def test_stale_quote_suppresses_holiday_session(self) -> None:
        now = datetime(2026, 8, 17, 10, 0)
        self.assertFalse(in_trading_session(now, [datetime(2026, 8, 14, 15, 0)]))
        self.assertTrue(in_trading_session(now, [datetime(2026, 8, 17, 9, 59)]))

    def test_worker_uses_dynamic_intervals(self) -> None:
        self.assertEqual(worker_interval(datetime(2026, 8, 17, 10, 0), 20), 20)
        self.assertEqual(worker_interval(datetime(2026, 8, 17, 12, 0), 20), 60)
        self.assertEqual(worker_interval(datetime(2026, 8, 16, 10, 0), 20), 300)

    def test_quiet_period_supports_cross_midnight(self) -> None:
        self.assertTrue(in_quiet_period(datetime(2026, 8, 17, 23, 0), "22:00-08:00"))
        self.assertTrue(in_quiet_period(datetime(2026, 8, 17, 7, 0), "22:00-08:00"))
        self.assertFalse(in_quiet_period(datetime(2026, 8, 17, 12, 0), "22:00-08:00"))

    def test_snapshot_round_trip_preserves_stock_code(self) -> None:
        source = pd.DataFrame([{"code": "002745", "price": 12.3}])
        restored = dataframe_from_payload(dataframe_to_payload(source))
        self.assertEqual(restored.iloc[0]["code"], "002745")


class NotificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.old_path = db.DB_PATH
        db.DB_PATH = Path(__file__).parent / ".test_watch.db"
        db.DB_PATH.unlink(missing_ok=True)
        db.init_db()

    def tearDown(self) -> None:
        db.DB_PATH = self.old_path
        (Path(__file__).parent / ".test_watch.db").unlink(missing_ok=True)

    def _event(self) -> AlertEvent:
        return AlertEvent("600000", "测试", "测试告警", "test", 1.0, 10.0)

    def test_disabled_notifications_are_skipped(self) -> None:
        fresh = filter_new_alerts([self._event()], notify_enabled=False)
        self.assertEqual(len(fresh), 1)
        self.assertEqual(db.recent_alerts(1)[0]["notify_status"], "skipped")

    def test_snapshot_health_and_runtime_config(self) -> None:
        db.save_quote_snapshot("[]", "测试源", 0, "2026-08-17 10:00:00")
        db.update_collector_health(status="healthy", pid=123, consecutive_failures=0)
        db.save_runtime_config({"refresh_sec": 20, "enable_push": True})
        self.assertEqual(db.load_quote_snapshot()["source"], "测试源")
        self.assertEqual(db.get_collector_health()["status"], "healthy")
        self.assertEqual(db.load_runtime_config()["refresh_sec"], 20)

    def test_instrument_settings_and_history(self) -> None:
        db.upsert_instrument_settings([{
            "code": "600000", "group_name": "银行", "enabled": True,
            "short_pct_threshold": 1.5, "volume_ratio_threshold": 2.0,
            "cooldown_minutes": 30, "severity": "紧急",
        }])
        setting = db.load_instrument_settings(["600000"])[0]
        self.assertEqual(setting["group_name"], "银行")
        self.assertEqual(setting["volume_ratio_threshold"], 2.0)
        db.save_quote_history([{"code": "600000", "price": 10}], "2026-08-17 10:00:00")
        self.assertEqual(db.recent_quote_history("600000")[0]["price"], 10)

    def test_cooldown_suppresses_duplicate(self) -> None:
        event = self._event()
        event.cooldown_minutes = 60
        self.assertEqual(len(filter_new_alerts([event], notify_enabled=False)), 1)
        duplicate = self._event()
        duplicate.cooldown_minutes = 60
        self.assertEqual(len(filter_new_alerts([duplicate], notify_enabled=False)), 0)

    @patch("worker.get_webhook", return_value="")
    @patch(
        "worker.collect_cycle",
        return_value={"duration_sec": 0.1, "captured_at": "2026-08-17 10:00:00"},
    )
    @patch("worker.load_watchlist", return_value=["600000"])
    def test_worker_once_updates_health(self, _codes, _cycle, _webhook) -> None:
        worker._running = True
        self.assertEqual(worker.run(once=True), 0)
        health = db.get_collector_health()
        self.assertEqual(health["status"], "stopped")
        self.assertEqual(health["consecutive_failures"], 0)

    @patch("notify.notify_alerts", return_value=(False, "网络失败"))
    def test_failed_notification_is_retained_for_retry(self, _mock) -> None:
        filter_new_alerts([self._event()])
        ok, _, count = deliver_pending_alerts("https://example.invalid")
        row = db.recent_alerts(1)[0]
        self.assertFalse(ok)
        self.assertEqual(count, 1)
        self.assertEqual(row["notify_status"], "failed")
        self.assertEqual(row["retry_count"], 1)
        self.assertIsNotNone(row["next_retry_at"])


if __name__ == "__main__":
    unittest.main()
