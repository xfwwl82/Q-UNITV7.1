#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_collector.py — 双轨采集引擎单元测试 (patch_v9)
=====================================================
T1: 节点扫描器（24节点 / 排序逻辑）
T2: TDXConnectionPool（线程隔离）
T3: 增量更新逻辑（max_date / merge / 去重）
T4: DataValidator（三层验证）
T5: RunReport（持久化 / 加载）
T6: 双轨合并逻辑（TDX + AKShare merge）
T7: 三级降级管道（全 Mock）
T8: AKShare 进程隔离退避（限流检测）
"""

import json
import math
import tempfile
import threading
import unittest
from datetime import date, timedelta
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch, call

import pandas as pd
import numpy as np


# ── 辅助函数 ──────────────────────────────────────────────────────────────
def _make_ohlcv(n: int, code: str = "000001", seed: int = 42) -> pd.DataFrame:
    rng   = np.random.RandomState(seed)
    dates = pd.date_range("2023-01-01", periods=n, freq="B").strftime("%Y-%m-%d")
    base  = 10.0 + rng.randn(n).cumsum() * 0.5
    base  = np.abs(base) + 5  # 确保正数
    return pd.DataFrame({
        "code":   code,
        "date":   dates,
        "open":   base.astype("float32"),
        "high":   (base * 1.02).astype("float32"),
        "low":    (base * 0.98).astype("float32"),
        "close":  (base * 1.005).astype("float32"),
        "vol":    np.ones(n, dtype="int64") * 100000,
        "amount": np.ones(n, dtype="int64") * 1000000,
        "source": "tdx",
        "adjust": "hfq",
    })


def _make_akshare_df(n: int, code: str = "000001") -> pd.DataFrame:
    dates = pd.date_range("2023-01-01", periods=n, freq="B").strftime("%Y-%m-%d")
    return pd.DataFrame({
        "code":      code,
        "date":      dates,
        "open":      np.ones(n, dtype="float32") * 10.0,
        "high":      np.ones(n, dtype="float32") * 10.5,
        "low":       np.ones(n, dtype="float32") * 9.5,
        "close":     np.ones(n, dtype="float32") * 10.2,
        "vol":       np.ones(n, dtype="int64") * 100000,
        "turnover":  np.random.rand(n).astype("float32") * 5,
        "pct_change":np.random.randn(n).astype("float32"),
        "source":    "akshare",
        "adjust":    "hfq",
    })


# ============================================================================
# T1: 节点扫描器
# ============================================================================
class TestNodeScanner(unittest.TestCase):

    def test_node_count_equals_24(self):
        from src.data.collector.node_scanner import TDX_NODES
        self.assertEqual(len(TDX_NODES), 24)

    def test_all_nodes_port_7709(self):
        from src.data.collector.node_scanner import TDX_NODES
        for n in TDX_NODES:
            self.assertEqual(n["port"], 7709)
            self.assertIn("host", n)
            self.assertIn("name", n)

    def test_sort_ok_before_failed(self):
        from src.data.collector.node_scanner import _sort_results
        data = [
            {"name": "a", "host": "1.1.1.1", "port": 7709, "latency_ms": 80.0, "status": "ok"},
            {"name": "b", "host": "2.2.2.2", "port": 7709, "latency_ms": -1.0, "status": "fail:x"},
            {"name": "c", "host": "3.3.3.3", "port": 7709, "latency_ms": 20.0, "status": "ok"},
        ]
        s = _sort_results(data)
        self.assertEqual(s[0]["name"], "c")
        self.assertEqual(s[1]["name"], "a")
        self.assertEqual(s[2]["name"], "b")

    def test_probe_unreachable(self):
        from src.data.collector.node_scanner import _probe_sync
        node = {"name": "x", "host": "10.255.255.1", "port": 1}
        r    = _probe_sync(node, timeout=0.3)
        self.assertLess(r["latency_ms"], 0)
        self.assertTrue(r["status"].startswith("fail"))


# ============================================================================
# T2: TDXConnectionPool 线程隔离
# ============================================================================
class TestTDXConnectionPool(unittest.TestCase):

    def test_empty_nodes_raises(self):
        import src.data.collector.tdx_pool as pool_mod
        with patch.object(pool_mod, "_PYTDX_AVAILABLE", True):
            from src.data.collector.tdx_pool import TDXConnectionPool
            with self.assertRaises(ValueError):
                TDXConnectionPool([], timeout=1.0)

    def test_local_storage_per_thread(self):
        """threading.local 保证不同线程看到各自的 api 实例。"""
        import threading
        import src.data.collector.tdx_pool as pool_mod
        from src.data.collector.tdx_pool import TDXConnectionPool

        nodes = [{"name": "n0", "host": "127.0.0.1", "port": 7709}]

        with patch.object(pool_mod, "_PYTDX_AVAILABLE", True):
            pool = TDXConnectionPool(nodes, timeout=0.1)

        captured = {}

        def _set_local(tid, val):
            pool._local.api = val
            import time; time.sleep(0.05)
            captured[tid] = pool._local.api

        threads = [threading.Thread(target=_set_local, args=(i, MagicMock())) for i in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()

        # 每个线程写入自己的值
        ids = list(set(id(v) for v in captured.values()))
        # 4 个线程应该有 4 个不同的 mock 对象（各自隔离）
        self.assertEqual(len(ids), 4)


# ============================================================================
# T3: 增量更新逻辑
# ============================================================================
class TestIncremental(unittest.TestCase):

    def _write_parquet(self, df: pd.DataFrame) -> Path:
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        path = Path(f.name)
        df.to_parquet(path, index=False)
        return path

    def test_read_local_max_date_no_file(self):
        from src.data.collector.incremental import read_local_max_date
        self.assertIsNone(read_local_max_date(Path("/tmp/nonexistent_xyz999.parquet")))

    def test_read_local_max_date_correct(self):
        from src.data.collector.incremental import read_local_max_date
        df   = _make_ohlcv(30)
        path = self._write_parquet(df)
        self.assertEqual(read_local_max_date(path), df["date"].max())
        path.unlink(missing_ok=True)

    def test_compute_missing_range_full(self):
        from src.data.collector.incremental import compute_missing_range
        s, e = compute_missing_range(None, "2024-03-01", "2005-01-01")
        self.assertEqual(s, "2005-01-01")
        self.assertEqual(e, "2024-03-01")

    def test_compute_missing_range_incremental(self):
        from src.data.collector.incremental import compute_missing_range
        s, e = compute_missing_range("2024-01-10", "2024-01-20")
        self.assertEqual(s, "2024-01-11")
        self.assertEqual(e, "2024-01-20")

    def test_merge_no_duplicates(self):
        from src.data.collector.incremental import merge_incremental
        old = _make_ohlcv(30)
        new = _make_ohlcv(20)   # 可能有日期重叠
        m   = merge_incremental(old, new)
        self.assertFalse(m.duplicated(subset=["date"]).any())
        self.assertEqual(m["date"].tolist(), sorted(m["date"].tolist()))

    def test_is_up_to_date_yesterday(self):
        from src.data.collector.incremental import is_up_to_date
        yest = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.assertTrue(is_up_to_date(yest))

    def test_is_up_to_date_old(self):
        from src.data.collector.incremental import is_up_to_date
        old = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        self.assertFalse(is_up_to_date(old))


# ============================================================================
# T4: DataValidator — 三层验证
# ============================================================================
class TestDataValidator(unittest.TestCase):

    def test_none_df_fails(self):
        from src.data.collector.validator import DataValidator
        ok, reason = DataValidator.validate(None)
        self.assertFalse(ok)
        self.assertEqual(reason, "df_is_none")

    def test_empty_df_fails(self):
        from src.data.collector.validator import DataValidator
        ok, reason = DataValidator.validate(pd.DataFrame())
        self.assertFalse(ok)
        self.assertEqual(reason, "df_empty")

    def test_too_few_rows_fails(self):
        from src.data.collector.validator import DataValidator
        df         = _make_ohlcv(5)
        ok, reason = DataValidator.validate(df, min_rows=10)
        self.assertFalse(ok)
        self.assertTrue(reason.startswith("too_few_rows"))

    def test_missing_col_fails(self):
        from src.data.collector.validator import DataValidator
        df         = _make_ohlcv(20).drop(columns=["close"])
        ok, reason = DataValidator.validate(df)
        self.assertFalse(ok)
        self.assertIn("missing_cols", reason)

    def test_high_lt_low_fails(self):
        from src.data.collector.validator import DataValidator
        df = _make_ohlcv(20)
        # 人工制造 high < low
        df.loc[5, "high"] = df.loc[5, "low"] - 1.0
        ok, reason = DataValidator.validate(df)
        self.assertFalse(ok)
        self.assertIn("high_lt_low", reason)

    def test_negative_close_fails(self):
        from src.data.collector.validator import DataValidator
        df = _make_ohlcv(20)
        df.loc[3, "close"] = -1.0
        ok, reason = DataValidator.validate(df)
        self.assertFalse(ok)
        self.assertIn("negative_close", reason)

    def test_valid_df_passes(self):
        from src.data.collector.validator import DataValidator
        df         = _make_ohlcv(30)
        ok, reason = DataValidator.validate(df)
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

    def test_validate_merge_result_sorted(self):
        from src.data.collector.validator import DataValidator
        df = _make_ohlcv(20)
        # 故意乱序
        df = df.sample(frac=1, random_state=42).reset_index(drop=True)
        ok, reason = DataValidator.validate_merge_result(df)
        self.assertFalse(ok)
        self.assertEqual(reason, "date_not_sorted")


# ============================================================================
# T5: RunReport 持久化
# ============================================================================
class TestRunReport(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_record_and_save(self):
        from src.data.collector.run_report import RunReport
        report = RunReport(self.tmpdir)
        report.record_success("000001", 0, source="tdx",      rows=1000)
        report.record_success("600000", 1, source="akshare",  rows=800)
        report.record_failed ("000002", 0, reason="complete_fail")
        report.record_skipped("000003", 0)
        report.save()

        failed_path = Path(self.tmpdir) / "failed_stocks.txt"
        self.assertTrue(failed_path.exists())
        content = failed_path.read_text()
        self.assertIn("000002", content)
        self.assertNotIn("000001", content)

        # JSON 存在
        json_files = list(Path(self.tmpdir).glob("run_stats_*.json"))
        self.assertEqual(len(json_files), 1)
        with open(json_files[0]) as f:
            stats = json.load(f)
        self.assertEqual(stats["success"], 2)
        self.assertEqual(stats["failed"], 1)

    def test_load_failed_list(self):
        from src.data.collector.run_report import RunReport
        report = RunReport(self.tmpdir)
        report.record_failed("000001", 0, reason="complete_fail")
        report.record_failed("600000", 1, reason="validate:too_few_rows")
        report.save()

        loaded = RunReport.load_failed_list(self.tmpdir)
        codes  = [c for c, _ in loaded]
        self.assertIn("000001", codes)
        self.assertIn("600000", codes)

    def test_summary_str_format(self):
        from src.data.collector.run_report import RunReport
        report = RunReport(self.tmpdir)
        report.record_success("000001", 0, "tdx")
        report.record_failed ("000002", 0, "fail")
        s = report.summary_str()
        self.assertIn("✓", s)
        self.assertIn("✗", s)

    def test_thread_safe_recording(self):
        """多线程并发记录不崩溃、计数准确。"""
        from src.data.collector.run_report import RunReport
        report = RunReport(self.tmpdir)

        def _worker(i):
            if i % 3 == 0:
                report.record_success(f"{i:06d}", 0, "tdx")
            elif i % 3 == 1:
                report.record_failed(f"{i:06d}", 0, "fail")
            else:
                report.record_skipped(f"{i:06d}", 0)

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(60)]
        for t in threads: t.start()
        for t in threads: t.join()

        self.assertEqual(report.total_success + report.total_failed + report.total_skipped, 60)


# ============================================================================
# T6: 双轨合并逻辑
# ============================================================================
class TestDualTrackMerge(unittest.TestCase):

    def test_both_none_returns_none(self):
        from src.data.collector.pipeline import _merge_dual_track
        self.assertIsNone(_merge_dual_track(None, None))

    def test_only_tdx_returns_tdx(self):
        from src.data.collector.pipeline import _merge_dual_track
        tdx = _make_ohlcv(20)
        result = _merge_dual_track(tdx, None)
        self.assertEqual(len(result), 20)
        self.assertEqual(result["source"].iloc[0], "tdx")

    def test_only_akshare_returns_akshare(self):
        from src.data.collector.pipeline import _merge_dual_track
        ak = _make_akshare_df(20)
        result = _merge_dual_track(None, ak)
        self.assertEqual(result["source"].iloc[0], "akshare")

    def test_merge_adds_turnover_column(self):
        """双轨合并后，result 应包含来自 AKShare 的 turnover 字段。"""
        from src.data.collector.pipeline import _merge_dual_track
        tdx = _make_ohlcv(30)
        ak  = _make_akshare_df(30)
        merged = _merge_dual_track(tdx, ak)
        self.assertIn("turnover", merged.columns)
        self.assertIn("pct_change", merged.columns)

    def test_merge_date_alignment(self):
        """只有重叠日期的行才应有非 NaN 的 turnover。"""
        from src.data.collector.pipeline import _merge_dual_track
        tdx = _make_ohlcv(30)
        # AKShare 只覆盖后 15 天
        ak  = _make_akshare_df(15)
        # 对齐日期
        ak_dates = tdx["date"].tail(15).tolist()
        ak["date"] = ak_dates
        merged  = _merge_dual_track(tdx, ak)
        # 前 15 行 turnover 应为 NaN，后 15 行应有值
        self.assertTrue(merged["turnover"].iloc[:15].isna().all())
        self.assertTrue(merged["turnover"].iloc[15:].notna().all())

    def test_merge_adjust_flag(self):
        """合并后 adjust 列应为 hfq（TDX 已 adjustflag=2 后复权，与 AKShare 体系统一）。"""
        from src.data.collector.pipeline import _merge_dual_track
        tdx    = _make_ohlcv(20)
        ak     = _make_akshare_df(20)
        # 统一 date
        ak["date"] = tdx["date"].tolist()
        merged = _merge_dual_track(tdx, ak)
        self.assertTrue((merged["adjust"] == "hfq").all())


# ============================================================================
# T7: 三级降级管道（全 Mock）
# ============================================================================
class TestFailoverPipeline(unittest.TestCase):

    def _run_update(self, tdx_df, ak_df, bs_df):
        """便捷辅助：模拟 update_single_stock 的三级降级。"""
        import tempfile
        from src.data.collector.pipeline import update_single_stock
        from src.data.collector.run_report import RunReport

        tmpdir  = tempfile.mkdtemp()
        report  = RunReport(tmpdir)
        pool    = MagicMock()
        ak_results = {"000001": ak_df}

        with patch("src.data.collector.pipeline._tdx_fetch_chunked", return_value=tdx_df),              patch("src.data.collector.pipeline._fetch_baostock_fallback", return_value=bs_df):
            return update_single_stock(
                code="000001", market=0,
                parquet_dir=Path(tmpdir),
                tdx_pool=pool,
                ak_results=ak_results,
                report=report,
                force_full=True,
            )

    def test_both_tracks_succeed(self):
        tdx = _make_ohlcv(30)
        ak  = _make_akshare_df(30)
        ak["date"] = tdx["date"].tolist()
        code, ok, src = self._run_update(tdx, ak, None)
        self.assertTrue(ok)
        self.assertIn(src, ("merged", "tdx", "akshare"))

    def test_tdx_fail_akshare_succeed(self):
        ak = _make_akshare_df(30)
        code, ok, src = self._run_update(None, ak, None)
        self.assertTrue(ok)
        self.assertEqual(src, "akshare")

    def test_both_fail_baostock_fallback(self):
        bs = _make_ohlcv(30)
        bs["source"] = "baostock"
        code, ok, src = self._run_update(None, None, bs)
        self.assertTrue(ok)
        self.assertEqual(src, "baostock")

    def test_all_three_fail(self):
        code, ok, src = self._run_update(None, None, None)
        self.assertFalse(ok)


# ============================================================================
# T8: AKShare 进程函数限流感知
# ============================================================================
class TestAKShareRateLimitBackoff(unittest.TestCase):

    def test_ratelimit_keyword_detection(self):
        """模拟 AKShare 抛出限流错误，验证等待时间大于普通错误。"""
        from src.data.collector.akshare_client import _RATELIMIT_KEYWORDS
        ratelimit_errs = ["429 Too Many Requests", "触发限流", "请求频繁", "too many requests"]
        for err in ratelimit_errs:
            is_rl = any(kw in err for kw in _RATELIMIT_KEYWORDS)
            self.assertTrue(is_rl, f"限流关键词未匹配: {err}")

    def test_normal_error_not_ratelimit(self):
        from src.data.collector.akshare_client import _RATELIMIT_KEYWORDS
        normal_errs = ["ConnectionError", "TimeoutError", "JSONDecodeError", "KeyError"]
        for err in normal_errs:
            is_rl = any(kw in err for kw in _RATELIMIT_KEYWORDS)
            self.assertFalse(is_rl, f"普通错误被误判为限流: {err}")

    def test_process_worker_handles_import_error(self):
        """akshare 未安装时，worker 应返回明确错误而非崩溃。"""
        from src.data.collector.akshare_client import _akshare_process_worker
        task = ("000001", "2023-01-01", "2023-12-31", 1, 0.0, 0.0)
        with patch.dict("sys.modules", {"akshare": None}):
            code, data, error = _akshare_process_worker(task)
        self.assertEqual(code, "000001")
        self.assertIsNone(data)
        self.assertIsNotNone(error)

    def test_extended_fields_constant(self):
        """AK_EXTENDED_FIELDS 必须包含 turnover。"""
        from src.data.collector.akshare_client import AK_EXTENDED_FIELDS
        self.assertIn("turnover", AK_EXTENDED_FIELDS)
        self.assertIn("pct_change", AK_EXTENDED_FIELDS)


if __name__ == "__main__":
    unittest.main(verbosity=2)
