#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pipeline.py — 双轨采集引擎 V7.1 修复版
========================================

【V7.1 修复：阶段顺序彻底反转】

原 op-v3 的性能陷阱:
  Phase 1: AKShare 全量串行  5190只 × 7s / 2进程 = 5+ 小时  ← 全程卡在这里
  Phase 2: TDX 多线程（等 Phase 1 全部完成才启动，pytdx 根本没有被调用！）

V7.1 修复方案:
  Phase 1: TDX 多线程并发（8线程，~30分钟）→ 立即流式写盘
  Phase 2: AKShare 可选扩展字段补充（enable_akshare=True 时启用）

【enable_akshare 参数】
  False（默认）: 仅 TDX，~30min，无 turnover/pct_change 字段
  True:          TDX 完成后再补 AKShare 扩展字段，~2~4h 额外

【推荐工作流】
  1. 全量建库: StockDataPipeline(enable_akshare=False).download_all_a_stocks()
  2. 按需补充: StockDataPipeline(enable_akshare=True).enrich_akshare()
  3. 每日增量: StockDataPipeline(enable_akshare=False).run(stock_list)

【复权】全程 hfq（后复权）
  TDX: adjustflag=2，老节点不支持时降级原始价并打印 WARNING
  AKShare: adjust="hfq"
  BaoStock: adjustflag="1"（两轨均失败时兜底）
"""

import time
import random
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

try:
    from tqdm import tqdm
    _TQDM_AVAILABLE = True
except ImportError:
    tqdm = None  # type: ignore[assignment,misc]
    _TQDM_AVAILABLE = False

from .node_scanner import get_fastest_nodes, TDX_NODES
from .tdx_pool import TDXConnectionPool
from .akshare_client import run_akshare_batch, AK_EXTENDED_FIELDS, fetch_akshare_single
from .baostock_client import fetch_baostock
from .incremental import (
    read_local_max_date, compute_missing_range,
    is_up_to_date, load_local_df, merge_incremental, save_df,
)
from .validator import DataValidator
from .run_report import RunReport

logger = logging.getLogger(__name__)

BARS_PER_REQ: int   = 800
TOTAL_BARS:   int   = 2500
SLEEP_MIN:    float = 0.1
SLEEP_MAX:    float = 0.2

try:
    from pytdx.params import TDXParams
    _KLINE_DAILY = TDXParams.KLINE_TYPE_DAILY
except ImportError:
    _KLINE_DAILY = 9


# ============================================================================
# TDX 分片下载（突破 800-bar 限制）
# ============================================================================

def _tdx_fetch_chunked(
    pool: TDXConnectionPool,
    code: str,
    market: int,
    missing_bars: int,
) -> Optional[pd.DataFrame]:
    """
    通过连接池分片拉取 TDX 日线数据。
    复权方式：优先 adjustflag=2（后复权 hfq），老节点不支持时降级并打印 WARNING。
    """
    api = pool.get_connection()
    if api is None:
        return None

    all_bars: List[dict] = []
    offset        = 0
    bars_to_fetch = min(missing_bars, TOTAL_BARS)

    try:
        while offset < bars_to_fetch:
            count = min(BARS_PER_REQ, bars_to_fetch - offset)
            try:
                bars = api.get_security_bars(
                    category=_KLINE_DAILY,
                    market=market,
                    code=code,
                    start=offset,
                    count=count,
                    adjustflag=2,
                )
            except Exception:
                logger.warning(
                    "%s 当前 TDX 节点不支持复权参数，回退至原始价，注意复权一致性风险！", code
                )
                bars = api.get_security_bars(
                    category=_KLINE_DAILY,
                    market=market,
                    code=code,
                    start=offset,
                    count=count,
                )

            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

            if not bars:
                break
            all_bars.extend(bars)
            if len(bars) < count:
                break
            offset += BARS_PER_REQ

    except Exception as exc:
        logger.warning("TDX 分片请求异常 (%s): %s", code, exc)
        pool.release()
        return None

    if not all_bars:
        return None

    return _clean_tdx_bars(all_bars, code, market)


def _clean_tdx_bars(raw: List[dict], code: str, market: int) -> pd.DataFrame:
    """向量化清洗 TDX 原始 bar 数据。"""
    df = pd.DataFrame(raw)
    col_map = {"datetime": "date", "vol": "vol", "amount": "amount"}
    df = df.rename(columns=col_map)
    std_cols = ["date", "open", "high", "low", "close", "vol", "amount"]
    df = df[[c for c in std_cols if c in df.columns]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
    for col in ("vol", "amount"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
    df.insert(0, "code",   code)
    df.insert(1, "market", market)
    df["source"] = "tdx"
    df["adjust"] = "hfq"
    return df.sort_values("date").reset_index(drop=True)


def _estimate_missing_bars(start_date: str, end_date: str) -> int:
    try:
        s = datetime.strptime(start_date, "%Y-%m-%d")
        e = datetime.strptime(end_date, "%Y-%m-%d")
        return max(1, int((e - s).days * 0.72)) + 1
    except Exception:
        return TOTAL_BARS


# ============================================================================
# 双轨合并
# ============================================================================

def _merge_dual_track(
    tdx_df: Optional[pd.DataFrame],
    ak_df: Optional[pd.DataFrame],
) -> Optional[pd.DataFrame]:
    """
    TDX OHLCV 与 AKShare 扩展字段合并（date 为键，两者均 hfq）。
    """
    if tdx_df is None and ak_df is None:
        return None
    if tdx_df is None:
        return ak_df
    if ak_df is None:
        return tdx_df

    extended = [c for c in ("turnover", "pct_change", "amplitude", "change")
                if c in ak_df.columns]
    if not extended:
        return tdx_df

    ak_ext = ak_df[["date"] + extended].copy()
    merged = pd.merge(tdx_df, ak_ext, on="date", how="left")
    merged["adjust"] = "hfq"
    return merged.sort_values("date").reset_index(drop=True)


# ============================================================================
# 单股完整更新流程
# ============================================================================

def update_single_stock(
    code: str,
    market: int,
    parquet_dir: Path,
    tdx_pool: TDXConnectionPool,
    ak_results: Dict[str, Optional[pd.DataFrame]],
    report: RunReport,
    force_full: bool = False,
) -> Tuple[str, bool, str]:
    """
    单股完整更新流程。
    ak_results 可为空字典（enable_akshare=False 时），此时仅用 TDX 数据。
    """
    parquet_path = parquet_dir / f"{code}.parquet"
    today_str    = date.today().strftime("%Y-%m-%d")

    local_max_date = None if force_full else read_local_max_date(parquet_path)

    if not force_full and is_up_to_date(local_max_date, today_str):
        report.record_skipped(code, market)
        return code, True, "skipped"

    start_date, end_date = compute_missing_range(local_max_date, today_str)
    if start_date == end_date and not force_full:
        report.record_skipped(code, market, reason="same_day")
        return code, True, "skipped"

    missing_bars = _estimate_missing_bars(start_date, end_date)

    # 轨道 A: TDX
    tdx_df: Optional[pd.DataFrame] = None
    try:
        raw = _tdx_fetch_chunked(tdx_pool, code, market, missing_bars)
        if raw is not None and not raw.empty:
            tdx_df = raw[raw["date"] >= start_date].copy()
            if tdx_df.empty:
                tdx_df = None
    except Exception as exc:
        logger.debug("TDX 轨道异常 (%s): %s", code, exc)

    # 轨道 B: AKShare（可能为空字典）
    ak_df: Optional[pd.DataFrame] = ak_results.get(code)
    if ak_df is not None and not ak_df.empty:
        ak_df = ak_df[ak_df["date"] >= start_date].copy()
        if ak_df.empty:
            ak_df = None

    # 双轨合并
    new_df = _merge_dual_track(tdx_df, ak_df)
    source_tag = "merged" if (tdx_df is not None and ak_df is not None) \
        else ("tdx" if ak_df is None else "akshare")

    # BaoStock 兜底
    if new_df is None:
        bs_df = fetch_baostock(code, market, start_date, end_date)
        if bs_df is not None and not bs_df.empty:
            new_df     = bs_df
            source_tag = "baostock"
        else:
            report.record_failed(code, market, reason="complete_fail")
            return code, False, "failed"

    # 数据验证
    ok, reason = DataValidator.validate(new_df, code=code)
    if not ok:
        logger.warning("数据验证失败 (%s): %s", code, reason)
        report.record_validation_failure(code, reason)
        report.record_failed(code, market, reason=f"validate:{reason}")
        return code, False, "validate_fail"

    # 增量合并
    if force_full:
        merged_df = new_df
    else:
        local_df  = load_local_df(parquet_path)
        merged_df = merge_incremental(local_df, new_df)

    ok2, reason2 = DataValidator.validate_merge_result(merged_df, code)
    if not ok2:
        logger.warning("合并后验证失败 (%s): %s", code, reason2)
        report.record_failed(code, market, reason=f"merge_validate:{reason2}")
        return code, False, "merge_validate_fail"

    # 流式写盘
    ok3 = save_df(merged_df, parquet_path)
    if not ok3:
        report.record_failed(code, market, reason="save_failed")
        return code, False, "save_failed"

    report.record_success(code, market, source=source_tag, rows=len(merged_df))
    return code, True, source_tag


# ============================================================================
# 主引擎：StockDataPipeline V7.1
# ============================================================================

class StockDataPipeline:
    """
    A 股日线采集引擎 V7.1 修复版。

    【V7.1 修复说明】
    op-v3 将 AKShare 放 Phase 1（5190只 × 7s / 2进程 = 5+小时全程阻塞），
    TDX 要等 AKShare 全部完成才启动。V7.1 修复为 TDX 先、AKShare 后。

    执行顺序 (enable_akshare=False，默认快速模式):
      Phase 1: TDX 8线程并发采集（~30分钟）→ 立即流式写盘

    执行顺序 (enable_akshare=True，完整双轨):
      Phase 1: TDX 8线程（~30分钟）→ 流式写盘
      Phase 2: AKShare 2进程补充 turnover/pct_change（~2~4小时）→ 合并写盘

    Args:
        enable_akshare: False=快速(仅TDX)，True=双轨(TDX+AKShare)
    """

    def __init__(
        self,
        parquet_dir:    str   = "./data/parquet",
        reports_dir:    str   = "./data/reports",
        top_n_nodes:    int   = 5,
        tdx_workers:    int   = 8,
        ak_workers:     int   = 2,
        node_timeout:   float = 3.0,
        tdx_timeout:    float = 10.0,
        ak_delay_min:   float = 0.3,
        ak_delay_max:   float = 0.8,
        ak_max_retries: int   = 3,
        force_full:     bool  = False,
        enable_akshare: bool  = False,  # V7.1: 默认关闭，全量时不阻塞
    ) -> None:
        self.parquet_dir    = Path(parquet_dir)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir    = reports_dir
        self.tdx_workers    = tdx_workers
        self.ak_workers     = ak_workers
        self.ak_delay_min   = ak_delay_min
        self.ak_delay_max   = ak_delay_max
        self.ak_max_retries = ak_max_retries
        self.force_full     = force_full
        self.enable_akshare = enable_akshare

        logger.info("节点赛马测试（%d 个候选节点）…", len(TDX_NODES))
        self.top_nodes = get_fastest_nodes(top_n=top_n_nodes, timeout=node_timeout)
        if self.top_nodes:
            logger.info(
                "选出节点: %s",
                [f"{n['name']}({n['latency_ms']:.0f}ms)" for n in self.top_nodes],
            )
        else:
            logger.warning("所有节点不可达")

        self.tdx_pool = TDXConnectionPool(top_nodes=self.top_nodes, timeout=tdx_timeout)

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------

    def run(self, stock_list: List[Tuple[str, int]]) -> Dict:
        """
        批量更新股票日线数据。

        V7.1 顺序: Phase1=TDX并发写盘（快速主轨）→ Phase2=AKShare可选扩展
        """
        total  = len(stock_list)
        report = RunReport(self.reports_dir)
        today  = date.today().strftime("%Y-%m-%d")

        mode_str = "双轨(TDX+AKShare)" if self.enable_akshare else "快速(仅TDX)"
        logger.info("V7.1 采集: %d 只 | 模式=%s | TDX=%d线程", total, mode_str, self.tdx_workers)

        # ── Phase 1: TDX 并发采集（快速主轨）── #
        logger.info("── Phase 1: TDX 并发采集 + 流式写盘（%d 只）…", total)

        t0   = time.perf_counter()
        pbar = None
        if _TQDM_AVAILABLE:
            pbar = tqdm(total=total,
                        desc=f"TDX采集[{self.tdx_workers}线程]",
                        unit="股", colour="green", dynamic_ncols=True)

        # Phase 1 中 AKShare 结果为空（不启动 AKShare 进程）
        empty_ak: Dict[str, Optional[pd.DataFrame]] = {}

        def _tdx_worker(args: Tuple[str, int]) -> Tuple[str, bool, str]:
            code, market = args
            return update_single_stock(
                code=code, market=market,
                parquet_dir=self.parquet_dir,
                tdx_pool=self.tdx_pool,
                ak_results=empty_ak,
                report=report,
                force_full=self.force_full,
            )

        with ThreadPoolExecutor(max_workers=self.tdx_workers) as executor:
            future_map = {executor.submit(_tdx_worker, task): task for task in stock_list}
            for future in as_completed(future_map):
                try:
                    _code, _ok, _src = future.result()
                except Exception as exc:
                    task = future_map[future]
                    logger.error("TDX Worker 异常 (%s): %s", task[0], exc)
                    report.record_failed(task[0], task[1], reason=f"worker_exception:{exc}")
                if pbar is not None:
                    pbar.update(1)
                    pbar.set_postfix_str(report.summary_str())

        if pbar is not None:
            pbar.close()

        t1 = time.perf_counter()
        logger.info("Phase 1 TDX 完成: %s | %.1fs | %.1f股/s",
                    report.summary_str(), t1 - t0,
                    total / (t1 - t0) if t1 > t0 else 0)

        # ── Phase 2: AKShare 可选扩展字段 ── #
        if self.enable_akshare:
            success_codes = set(report._success.keys())
            ak_targets = [(c, m) for c, m in stock_list if c in success_codes]
            if ak_targets:
                logger.info("── Phase 2: AKShare 扩展字段补充（%d 只）…", len(ak_targets))
                self._enrich_akshare_phase(ak_targets, report)
            else:
                logger.info("── Phase 2: 无成功股票，跳过 AKShare")

        report.save()

        elapsed = time.perf_counter() - t0
        result = {
            "total":           total,
            "success":         report.total_success,
            "failed":          report.total_failed,
            "skipped":         report.total_skipped,
            "elapsed_s":       round(elapsed, 2),
            "speed":           round(total / elapsed, 2) if elapsed > 0 else 0,
            "reports_dir":     str(self.reports_dir),
            "akshare_enabled": self.enable_akshare,
        }
        logger.info("采集完成: %s | %.1fs | %.1f股/s",
                    report.summary_str(), elapsed, result["speed"])
        return result

    # ------------------------------------------------------------------
    # AKShare 扩展字段独立补充（可在 TDX 采集完成后单独调用）
    # ------------------------------------------------------------------

    def enrich_akshare(self, stock_list: Optional[List[Tuple[str, int]]] = None) -> Dict:
        """
        对本地 Parquet 文件补充 AKShare 扩展字段（turnover / pct_change）。
        可在 TDX 快速模式采集完成后单独调用，无需重跑 TDX。

        Args:
            stock_list: [(code, market), ...] 若为 None 则自动扫描本地所有 Parquet

        Returns:
            统计字典 {total, success, failed}
        """
        if stock_list is None:
            codes = [f.stem for f in self.parquet_dir.glob("*.parquet")]
            stock_list = []
            for code in codes:
                market = 1 if code.startswith(("60", "68")) else 0
                stock_list.append((code, market))

        if not stock_list:
            logger.info("enrich_akshare: 无可补充的股票")
            return {"total": 0, "success": 0, "failed": 0}

        report = RunReport(self.reports_dir)
        logger.info("AKShare 扩展字段补充（独立模式）: %d 只", len(stock_list))
        self._enrich_akshare_phase(stock_list, report)
        report.save()
        return {
            "total":   len(stock_list),
            "success": report.total_success,
            "failed":  report.total_failed,
        }

    def _enrich_akshare_phase(
        self,
        stock_list: List[Tuple[str, int]],
        report: RunReport,
    ) -> None:
        """
        内部：AKShare 进程池批量采集扩展字段，再 LEFT JOIN 写入本地 Parquet。
        """
        today = date.today().strftime("%Y-%m-%d")

        ak_tasks = []
        for code, _market in stock_list:
            parquet_path   = self.parquet_dir / f"{code}.parquet"
            local_max_date = read_local_max_date(parquet_path)
            start_date, end_date = compute_missing_range(local_max_date, today)
            if start_date != end_date or self.force_full:
                ak_tasks.append((code, start_date, end_date))

        if not ak_tasks:
            logger.info("AKShare 扩展字段: 所有股票已是最新，跳过")
            return

        ak_pbar = None
        if _TQDM_AVAILABLE:
            ak_pbar = tqdm(total=len(ak_tasks), desc="AKShare扩展字段",
                           unit="股", colour="blue", dynamic_ncols=True)

        def _ak_cb(code: str, success: bool, error) -> None:
            if ak_pbar:
                ak_pbar.update(1)
                ak_pbar.set_postfix_str(f"{'OK' if success else 'NG'} {code}")

        ak_results = run_akshare_batch(
            stock_list=ak_tasks,
            max_workers=self.ak_workers,
            max_retries=self.ak_max_retries,
            delay_min=self.ak_delay_min,
            delay_max=self.ak_delay_max,
            progress_callback=_ak_cb,
        )
        if ak_pbar:
            ak_pbar.close()

        ak_ok = sum(1 for v in ak_results.values() if v is not None)
        logger.info("AKShare 扩展字段采集完成: %d/%d 成功", ak_ok, len(ak_tasks))

        # 将 AKShare 扩展字段 LEFT JOIN 写回本地 Parquet
        merged_count = 0
        for code, ak_df in ak_results.items():
            if ak_df is None or ak_df.empty:
                continue
            try:
                parquet_path = self.parquet_dir / f"{code}.parquet"
                local_df = load_local_df(parquet_path)
                if local_df is None or local_df.empty:
                    continue
                extended = [c for c in ("turnover", "pct_change", "amplitude", "change")
                            if c in ak_df.columns]
                if not extended:
                    continue
                ak_ext   = ak_df[["date"] + extended].copy()
                drop_old = [c for c in extended if c in local_df.columns]
                if drop_old:
                    local_df = local_df.drop(columns=drop_old)
                merged_df = pd.merge(local_df, ak_ext, on="date", how="left")
                ok_v, _ = DataValidator.validate_merge_result(merged_df, code)
                if ok_v:
                    save_df(merged_df, parquet_path)
                    merged_count += 1
                    report.record_success(code, 0, source="akshare_enrich",
                                          rows=len(merged_df))
            except Exception as exc:
                logger.warning("AKShare 扩展字段写入失败 (%s): %s", code, exc)

        logger.info("AKShare 扩展字段写入 %d 只股票", merged_count)

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    def download_all_a_stocks(self) -> Dict:
        """一键下载全量 A 股。"""
        stock_list = self._get_all_a_stock_list()
        if not stock_list:
            logger.error("获取 A 股列表失败")
            return {}
        return self.run(stock_list)

    def retry_failed(self, reports_dir: Optional[str] = None) -> Dict:
        """从 failed_stocks.txt 加载失败列表，重新采集。"""
        rdir = reports_dir or self.reports_dir
        failed_list = RunReport.load_failed_list(rdir)
        if not failed_list:
            logger.info("failed_stocks.txt 为空或不存在，无需补采")
            return {"total": 0}
        logger.info("开始补采 %d 只失败股票…", len(failed_list))
        old_force = self.force_full
        self.force_full = True
        result = self.run(failed_list)
        self.force_full = old_force
        return result

    def _get_all_a_stock_list(self) -> List[Tuple[str, int]]:
        """通过 TDX 获取全量 A 股代码列表。"""
        api = self.tdx_pool.get_connection()
        if api is None:
            return []
        try:
            stocks: List[Tuple[str, int]] = []
            for market in (0, 1):
                count = api.get_security_count(market)
                logger.info("市场 %d 总记录数: %d", market, count)
                for start in range(0, count, 1000):
                    batch = api.get_security_list(market, start)
                    if not batch:
                        continue
                    for item in batch:
                        code = item["code"]
                        if market == 0 and code.startswith(("00", "30")):
                            stocks.append((code, market))
                        elif market == 1 and code.startswith(("60", "68")):
                            stocks.append((code, market))
            stocks = list(set(stocks))
            logger.info("A 股总数: %d", len(stocks))
            return stocks
        except Exception as exc:
            logger.error("获取股票列表失败: %s", exc)
            return []
