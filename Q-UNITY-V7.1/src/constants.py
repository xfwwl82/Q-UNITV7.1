#!/usr/bin/env python3
"""
Q-UNITY-V7.1 全局常量定义
整合 V6 基础版常量 + V6-op patch_v9 复权标准
"""

VERSION    = "7.1.0"
BUILD_DATE = "2026-02-23"

# 复权标准（全局统一，hfq=后复权）
ADJUST_STD = "hfq"

T_PLUS_1       = True
MIN_TRADE_UNIT = 100

DEFAULT_COMMISSION_RATE = 0.0003
STAMP_TAX_RATE          = 0.001
DEFAULT_SLIPPAGE_RATE   = 0.001
MIN_COMMISSION          = 5.0

MAX_DRAWDOWN      = 0.20
MAX_POSITION_PCT  = 0.10
MAX_INDUSTRY_PCT  = 0.30
STOP_LOSS_PCT     = 0.10
TAKE_PROFIT_PCT   = 0.20
TRAILING_STOP_PCT = 0.05

# ==================== RSRS 因子阈值说明 (NB-17) ====================
RSRS_NORMALIZED_NOTE = "归一化beta，阈值需重新校准，详见constants.py注释"

TRADING_DAYS_PER_YEAR = 252
MIN_HISTORY_DAYS      = 100
