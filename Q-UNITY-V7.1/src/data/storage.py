#!/usr/bin/env python3
"""
Q-UNITY-V6 列式存储管理器
支持 Parquet（优先）和 CSV.gz（降级）两种格式
"""
from __future__ import annotations
import logging
import gzip
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

try:
    import pyarrow  # noqa
    _PARQUET = True
except ImportError:
    _PARQUET = False
    logger.warning("PyArrow 未安装，自动降级为 CSV.gz 格式")


class ColumnarStorageManager:
    """列式存储管理器：统一存取接口，透明格式切换"""

    def __init__(self, base_dir: str = "./data") -> None:
        self.base_dir = Path(base_dir)
        self.stock_dir  = self.base_dir / "parquet"
        self.factor_dir = self.base_dir / "factors"
        self.industry_dir = self.base_dir / "industry"
        for d in [self.stock_dir, self.factor_dir, self.industry_dir]:
            d.mkdir(parents=True, exist_ok=True)
        self.gateway = None

    # ── 股票行情 ────────────────────────────────────────────────────────────

    def _stock_path(self, code: str, fmt: str = None) -> Path:
        if fmt is None:
            fmt = "parquet" if _PARQUET else "csv.gz"
        return self.stock_dir / f"{code}.{fmt}"

    def save_stock_data(self, code: str, df: pd.DataFrame) -> bool:
        if df is None or df.empty:
            return False
        try:
            if _PARQUET:
                df.to_parquet(self._stock_path(code, "parquet"), engine="pyarrow")
            else:
                with gzip.open(self._stock_path(code, "csv.gz"), "wt", encoding="utf-8") as f:
                    df.to_csv(f)
            return True
        except Exception as e:
            logger.error(f"保存 {code} 失败: {e}")
            return False

    def load_stock_data(
        self, code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        try:
            if _PARQUET:
                path = self._stock_path(code, "parquet")
                if not path.exists():
                    path = self._stock_path(code, "csv.gz")
                    if not path.exists():
                        return None
                    df = pd.read_csv(path, index_col=0, parse_dates=True)
                else:
                    df = pd.read_parquet(path, engine="pyarrow")
            else:
                path = self._stock_path(code, "csv.gz")
                if not path.exists():
                    return None
                df = pd.read_csv(path, index_col=0, parse_dates=True)

            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            if start_date:
                df = df.loc[df.index >= pd.to_datetime(start_date)]
            if end_date:
                df = df.loc[df.index <= pd.to_datetime(end_date)]
            return df if not df.empty else None
        except Exception as e:
            logger.debug(f"加载 {code} 失败: {e}")
            return None

    def get_all_codes(self) -> List[str]:
        codes = set()
        for ext in ("parquet", "csv.gz"):
            for p in self.stock_dir.glob(f"*.{ext}"):
                codes.add(p.name.split(".")[0])
        return sorted(codes)

    # ── 因子数据 ────────────────────────────────────────────────────────────

    def save_factor_data(self, code: str, df: pd.DataFrame) -> bool:
        if df is None or df.empty:
            return False
        try:
            path = self.factor_dir / f"{code}_factors.parquet"
            if _PARQUET:
                df.to_parquet(path)
            else:
                df.to_csv(path.with_suffix(".csv.gz"), compression="gzip")
            return True
        except Exception as e:
            logger.error(f"保存因子 {code} 失败: {e}")
            return False

    def load_factor_data(self, code: str) -> Optional[pd.DataFrame]:
        try:
            path = self.factor_dir / f"{code}_factors.parquet"
            if not path.exists():
                path = self.factor_dir / f"{code}_factors.csv.gz"
                if not path.exists():
                    return None
                return pd.read_csv(path, index_col=0, parse_dates=True)
            return pd.read_parquet(path)
        except Exception:
            return None

    # ── 行业数据 ────────────────────────────────────────────────────────────

    def save_industry_data(self, df: pd.DataFrame) -> bool:
        try:
            path = self.industry_dir / "industry_map.parquet"
            if _PARQUET:
                df.to_parquet(path)
            else:
                df.to_csv(path.with_suffix(".csv"), index=False)
            return True
        except Exception as e:
            logger.error(f"保存行业数据失败: {e}")
            return False

    def load_industry_data(self) -> Optional[pd.DataFrame]:
        try:
            for path in [
                self.industry_dir / "industry_map.parquet",
                self.industry_dir / "industry_map.csv",
            ]:
                if path.exists():
                    if path.suffix == ".parquet":
                        return pd.read_parquet(path)
                    return pd.read_csv(path)
        except Exception:
            pass
        return None
