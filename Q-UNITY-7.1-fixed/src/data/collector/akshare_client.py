#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
akshare_client.py — AKShare 进程隔离采集客户端 (patch_v9)
==========================================================

【设计决策说明】
AKShare 底层使用 requests.Session 并在模块级维护全局 HTTP 会话状态。
多线程并发调用时，多个线程共享同一进程的全局 session，导致：
  - Cookie/Header 状态竞争 → 服务端风控误判
  - 同一 session 并发请求被识别为同一来源 → 限流概率指数级上升
  - 偶发性响应错位（线程 A 的响应被线程 B 读取）

因此，AKShare 必须在独立子进程中调用，每个子进程拥有完全独立的 session。
本模块提供：
  1. _akshare_process_worker()  — 顶层可序列化函数，在子进程中运行
  2. run_akshare_batch()        — ProcessPoolExecutor 批量调度接口
  3. fetch_akshare_single()     — 单股同步接口（主进程调用，供测试用）

【复权方式】统一 hfq（后复权）
量化回测必须使用后复权：历史价格连续，动量/反转因子准确，回测结果可重现。
前复权（qfq）会随每次除权事件重写历史数据，导致回测不一致。

【限流感知退避策略】
东方财富接口限流特征字符串：429 / 限流 / 频繁 / too many
触发限流时等待 30s/60s/90s（远长于普通错误的 1s/2s/4s）。
"""

import time
import random
import logging
from typing import Optional, Tuple, List, Dict, Any
from concurrent.futures import ProcessPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# 东方财富限流特征字符串（AKShare 底层爬取东方财富 API）
_RATELIMIT_KEYWORDS = ("429", "限流", "频繁", "too many", "Too Many", "rate limit")

# AKShare → 标准列名映射
_AK_COL_MAP = {
    "日期":  "date",
    "开盘":  "open",
    "收盘":  "close",
    "最高":  "high",
    "最低":  "low",
    "成交量": "vol",
    "成交额": "amount",
    "振幅":  "amplitude",
    "涨跌幅": "pct_change",
    "涨跌额": "change",
    "换手率": "turnover",
}

# 扩展字段（TDX 不提供，AKShare 专属）
AK_EXTENDED_FIELDS = {"amplitude", "pct_change", "change", "turnover"}


# ============================================================================
# 子进程工作函数（必须是顶层函数，才能被 pickle 序列化传入子进程）
# ============================================================================

def _akshare_process_worker(
    task: Tuple[str, str, str, int, float, float]
) -> Tuple[str, Optional[Any], Optional[str]]:
    """
    在独立子进程中采集单只股票的 AKShare 数据。

    Args:
        task: (code, start_date, end_date, max_retries, delay_min, delay_max)

    Returns:
        (code, df_dict_or_None, error_msg_or_None)
        注意：DataFrame 不能直接 pickle，通过 to_dict("records") 传回主进程。
    """
    # ── 子进程级独立 import，获得完全独立的 session ──────────────────
    try:
        import akshare as ak
    except ImportError:
        return (task[0], None, "akshare_not_installed")

    import pandas as pd

    code, start_date, end_date, max_retries, delay_min, delay_max = task

    # 统一日期格式为 YYYYMMDD（AKShare 要求）
    start_fmt = start_date.replace("-", "")
    end_fmt   = end_date.replace("-", "")

    for attempt in range(max_retries):
        try:
            # 随机延迟（防封）
            time.sleep(random.uniform(delay_min, delay_max))

            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start_fmt,
                end_date=end_fmt,
                adjust="hfq",   # 后复权，量化回测标准
            )

            if df is None or df.empty:
                raise ValueError(f"返回空数据")

            # 列名标准化
            df = df.rename(columns=_AK_COL_MAP)
            df["code"]   = code
            df["source"] = "akshare"
            df["adjust"] = "hfq"

            # date 格式统一
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

            # 数值类型
            for col in ("open", "high", "low", "close"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")
            for col in ("vol", "amount"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
            for col in ("pct_change", "turnover", "amplitude", "change"):
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float32")

            # 通过 dict 传回（DataFrame 无法跨进程 pickle）
            return (code, df.to_dict("records"), None)

        except Exception as exc:
            err_str = str(exc)
            # 限流检测
            is_ratelimit = any(kw in err_str for kw in _RATELIMIT_KEYWORDS)
            if is_ratelimit:
                wait = 30.0 * (attempt + 1)   # 30s / 60s / 90s
            else:
                wait = (2 ** attempt) * (1 + random.random() * 0.3)  # 1s / 2s / 4s + 抖动

            if attempt < max_retries - 1:
                time.sleep(wait)
            else:
                return (code, None, f"all_retries_failed:{err_str[:120]}")

    return (code, None, "unknown_error")


# ============================================================================
# 批量调度接口（主进程调用）
# ============================================================================

def run_akshare_batch(
    stock_list: List[Tuple[str, str, str]],
    max_workers: int = 2,
    max_retries: int = 3,
    delay_min: float = 0.3,
    delay_max: float = 0.8,
    progress_callback=None,
) -> Dict[str, Optional[Any]]:
    """
    使用 ProcessPoolExecutor 批量采集 AKShare 数据。

    Args:
        stock_list:        [(code, start_date, end_date), ...]
        max_workers:       进程数（建议 2，东方财富接口并发限制严格）
        max_retries:       单股最大重试次数
        delay_min/max:     子进程内随机 sleep 区间（秒）
        progress_callback: fn(code, success, error) 进度回调

    Returns:
        {code: df_or_None}  DataFrame 格式
    """
    import pandas as pd

    if not stock_list:
        return {}

    # 构建任务 tuple
    tasks = [
        (code, start, end, max_retries, delay_min, delay_max)
        for code, start, end in stock_list
    ]

    results: Dict[str, Optional[Any]] = {}

    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        future_map = {
            pool.submit(_akshare_process_worker, task): task[0]
            for task in tasks
        }
        for future in as_completed(future_map):
            code = future_map[future]
            try:
                result_code, data, error = future.result()
                if data is not None:
                    # 将 dict 重建为 DataFrame
                    df = pd.DataFrame(data)
                    if "date" in df.columns:
                        df = df.sort_values("date").reset_index(drop=True)
                    results[code] = df
                    if progress_callback:
                        progress_callback(code, True, None)
                else:
                    results[code] = None
                    logger.warning("AKShare 采集失败 %s: %s", code, error)
                    if progress_callback:
                        progress_callback(code, False, error)
            except Exception as exc:
                results[code] = None
                logger.error("AKShare worker 异常 %s: %s", code, exc)
                if progress_callback:
                    progress_callback(code, False, str(exc))

    return results


# ============================================================================
# 单股同步接口（主进程直接调用，供降级 fallback 和测试用）
# ============================================================================

def fetch_akshare_single(
    code: str,
    start_date: str,
    end_date: str,
    max_retries: int = 3,
    delay_min: float = 0.3,
    delay_max: float = 0.8,
) -> Optional[Any]:
    """
    同步采集单只股票（直接在调用进程中运行）。
    注意：此接口仅供单线程场景使用，多线程并发调用有 session 竞争风险。

    Returns:
        pd.DataFrame 或 None
    """
    import pandas as pd

    task = (code, start_date, end_date, max_retries, delay_min, delay_max)
    result_code, data, error = _akshare_process_worker(task)
    if data is None:
        return None
    df = pd.DataFrame(data)
    if "date" in df.columns:
        df = df.sort_values("date").reset_index(drop=True)
    return df
