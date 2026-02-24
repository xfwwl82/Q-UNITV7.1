#!/usr/bin/env python3
"""
Q-UNITY-V6 策略全集（8大策略）
  1. RSRSMomentumStrategy     — 基础RSRS动量
  2. AlphaHunterStrategy      — 高频多层锁
  3. RSRSAdvancedStrategy     — R²过滤+量价共振
  4. ShortTermStrategy        — 快进快出+日历止时 (NB-14)
  5. MomentumReversalStrategy — 双模式 60/40
  6. SentimentReversalStrategy— 超卖反转
  7. KunpengV10Strategy       — 微结构(聪明钱+稳定非流动性+缺口惩罚)+宽度熔断
  8. AlphaMaxV5FixedStrategy  — 机构多因子(EP/成长/动量/质量/REV/流动/残差波动)+行业中性+风险平价
"""
from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd

from ..types import OrderSide, Signal

logger = logging.getLogger(__name__)


# ============================================================================
# 基类
# ============================================================================

class BaseStrategy(ABC):
    """策略基类"""

    name: str = "BaseStrategy"

    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
        self._state: Dict[str, Any] = {}

    @abstractmethod
    def generate_signals(
        self,
        universe: List[str],
        market_data: Dict[str, pd.DataFrame],
        factor_data: Dict[str, pd.DataFrame],
        current_date: datetime,
        positions: Dict[str, Any],
    ) -> List[Signal]:
        ...

    def _make_buy(self, code: str, score: float, weight: float,
                  ts: datetime, reason: str = "") -> Signal:
        return Signal(timestamp=ts, code=code, side=OrderSide.BUY,
                      score=score, weight=weight, reason=reason,
                      strategy_name=self.name)

    def _make_sell(self, code: str, ts: datetime, reason: str = "") -> Signal:
        return Signal(timestamp=ts, code=code, side=OrderSide.SELL,
                      score=0.0, weight=0.0, reason=reason,
                      strategy_name=self.name)


# ============================================================================
# 1. RSRSMomentumStrategy
# ============================================================================

class RSRSMomentumStrategy(BaseStrategy):
    """
    RSRS 动量策略
    - 买入: rsrs_adaptive > threshold（买入前N只）
    - 卖出: rsrs_adaptive < -threshold
    """
    name = "RSRSMomentum"

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)
        self.top_n = self.config.get("top_n", 10)
        self.rsrs_threshold = self.config.get("rsrs_threshold", 0.5)

    def generate_signals(self, universe, market_data, factor_data,
                         current_date, positions) -> List[Signal]:
        signals = []
        scores = {}

        for code in universe:
            fd = factor_data.get(code)
            if fd is None or fd.empty or "rsrs_adaptive" not in fd.columns:
                continue
            # NB-01: 使用 T-1 数据（截止 current_date 前一行）
            vals = fd["rsrs_adaptive"].dropna()
            if vals.empty:
                continue
            v = float(vals.iloc[-1])
            if v > self.rsrs_threshold:
                scores[code] = v

        # 退出信号
        for code in list(positions.keys()):
            fd = factor_data.get(code)
            if fd is None or fd.empty:
                continue
            vals = fd.get("rsrs_adaptive", pd.Series()).dropna()
            if vals.empty:
                continue
            if float(vals.iloc[-1]) < -self.rsrs_threshold:
                signals.append(self._make_sell(code, current_date, "RSRS跌破下限"))

        # 买入信号（Top-N）
        top = sorted(scores, key=scores.__getitem__, reverse=True)[: self.top_n]
        weight = 1.0 / max(len(top), 1)
        for code in top:
            if code not in positions:
                signals.append(self._make_buy(code, scores[code], weight,
                                              current_date, "RSRS强势"))
        return signals


# ============================================================================
# 2. AlphaHunterStrategy
# ============================================================================

class AlphaHunterStrategy(BaseStrategy):
    """
    Alpha 猎手策略（多层评分锁定）
    综合 rsrs_adaptive + mom + vol_factor 三因子加权打分
    """
    name = "AlphaHunter"

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)
        self.top_n    = self.config.get("top_n", 15)
        self.min_score = self.config.get("min_score", 0.3)
        self.factor_weights = self.config.get("factor_weights", {
            "rsrs_adaptive": 0.5, "mom": 0.3, "vol_factor": 0.2,
        })

    def _get_score(self, fd: pd.DataFrame) -> float:
        score = 0.0
        total_w = 0.0
        for fn, w in self.factor_weights.items():
            if fn in fd.columns:
                vals = fd[fn].dropna()
                if not vals.empty:
                    score += float(vals.iloc[-1]) * w
                    total_w += w
        return score / total_w if total_w > 0 else 0.0

    def generate_signals(self, universe, market_data, factor_data,
                         current_date, positions) -> List[Signal]:
        signals = []
        scores = {}

        for code in universe:
            fd = factor_data.get(code)
            if fd is not None and not fd.empty:
                s = self._get_score(fd)
                if s >= self.min_score:
                    scores[code] = s

        # 退出低分持仓
        for code in list(positions.keys()):
            fd = factor_data.get(code)
            if fd is not None and not fd.empty:
                s = self._get_score(fd)
                if s < -self.min_score:
                    signals.append(self._make_sell(code, current_date, f"多因子分数{s:.2f}"))

        top = sorted(scores, key=scores.__getitem__, reverse=True)[: self.top_n]
        weight = 1.0 / max(len(top), 1)
        for code in top:
            if code not in positions:
                signals.append(self._make_buy(code, scores[code], weight,
                                              current_date, f"多因子{scores[code]:.2f}"))
        return signals


# ============================================================================
# 3. RSRSAdvancedStrategy
# ============================================================================

class RSRSAdvancedStrategy(BaseStrategy):
    """
    高级RSRS策略: R²过滤 + 量价共振确认
    - 仅在 rsrs_r2 > r2_threshold 时买入
    - 量价共振: turnover 需高于均值
    """
    name = "RSRSAdvanced"

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)
        self.top_n         = self.config.get("top_n", 10)
        self.rsrs_threshold = self.config.get("rsrs_threshold", 0.5)
        self.r2_threshold  = self.config.get("r2_threshold", 0.7)

    def generate_signals(self, universe, market_data, factor_data,
                         current_date, positions) -> List[Signal]:
        signals = []
        candidates = {}

        for code in universe:
            fd = factor_data.get(code)
            if fd is None or fd.empty:
                continue
            ra = fd.get("rsrs_adaptive", pd.Series()).dropna()
            r2 = fd.get("rsrs_r2", pd.Series()).dropna()
            if ra.empty or r2.empty:
                continue
            # R² 过滤
            if float(r2.iloc[-1]) < self.r2_threshold:
                continue
            v = float(ra.iloc[-1])
            if v > self.rsrs_threshold:
                # 量价共振
                if "turnover" in fd.columns:
                    to = fd["turnover"].dropna()
                    if not to.empty and float(to.iloc[-1]) < 1.0:
                        continue   # 换手低，跳过
                candidates[code] = v

        for code in list(positions.keys()):
            fd = factor_data.get(code)
            if fd is not None and not fd.empty:
                ra = fd.get("rsrs_adaptive", pd.Series()).dropna()
                if not ra.empty and float(ra.iloc[-1]) < -self.rsrs_threshold:
                    signals.append(self._make_sell(code, current_date, "RSRS高级退出"))

        top = sorted(candidates, key=candidates.__getitem__, reverse=True)[: self.top_n]
        weight = 1.0 / max(len(top), 1)
        for code in top:
            if code not in positions:
                signals.append(self._make_buy(code, candidates[code], weight,
                                              current_date, f"RSRS+R²+量价共振"))
        return signals


# ============================================================================
# 4. ShortTermStrategy  (NB-14 日历日止时)
# ============================================================================

class ShortTermStrategy(BaseStrategy):
    """
    短线快进快出策略
    NB-14: 时间止损基于日历日（不是交易日）
    """
    name = "ShortTerm"

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)
        self.top_n    = self.config.get("top_n", 5)
        self.hold_calendar_days = self.config.get("hold_calendar_days", 7)  # NB-14
        self.mom_threshold = self.config.get("mom_threshold", 0.03)

    def generate_signals(self, universe, market_data, factor_data,
                         current_date, positions) -> List[Signal]:
        signals = []
        scores = {}

        # NB-14: 日历日时间止损检查
        for code, pos in positions.items():
            entry = getattr(pos, "entry_date", None)
            if entry is not None:
                held_calendar = (current_date - entry).days   # 日历日
                if held_calendar >= self.hold_calendar_days:
                    signals.append(self._make_sell(code, current_date,
                                                   f"时间止损{held_calendar}日历日"))
                    continue
            # 动量退出
            fd = factor_data.get(code)
            if fd is not None and "mom" in fd.columns:
                m = fd["mom"].dropna()
                if not m.empty and float(m.iloc[-1]) < -self.mom_threshold:
                    signals.append(self._make_sell(code, current_date, "动量反转"))

        for code in universe:
            fd = factor_data.get(code)
            if fd is None or fd.empty or "mom" not in fd.columns:
                continue
            m = fd["mom"].dropna()
            if m.empty:
                continue
            v = float(m.iloc[-1])
            if v > self.mom_threshold:
                scores[code] = v

        top = sorted(scores, key=scores.__getitem__, reverse=True)[: self.top_n]
        weight = 1.0 / max(len(top), 1)
        for code in top:
            if code not in positions:
                signals.append(self._make_buy(code, scores[code], weight,
                                              current_date, "短线动量"))
        return signals


# ============================================================================
# 5. MomentumReversalStrategy
# ============================================================================

class MomentumReversalStrategy(BaseStrategy):
    """双模式: 强势市场追动量(60%) / 弱势市场做反转(40%)"""
    name = "MomentumReversal"

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)
        self.top_n = self.config.get("top_n", 10)
        self.market_thresh = self.config.get("market_thresh", 0.0)

    def _get_market_mode(self, market_data: Dict) -> str:
        mom_list = []
        for code, df in market_data.items():
            if "close" in df.columns and len(df) > 20:
                ret = float(df["close"].iloc[-1] / df["close"].iloc[-20] - 1)
                mom_list.append(ret)
        if not mom_list:
            return "neutral"
        avg = np.mean(mom_list)
        return "bull" if avg > self.market_thresh else "bear"

    def generate_signals(self, universe, market_data, factor_data,
                         current_date, positions) -> List[Signal]:
        mode = self._get_market_mode(market_data)
        signals = []
        scores = {}

        for code in universe:
            fd = factor_data.get(code)
            if fd is None or fd.empty:
                continue
            m = fd.get("mom", pd.Series()).dropna()
            if m.empty:
                continue
            v = float(m.iloc[-1])
            if mode == "bull":
                if v > 0:
                    scores[code] = v      # 追动量
            else:
                if v < -0.05:
                    scores[code] = -v     # 做反转（超卖）

        for code in list(positions.keys()):
            if code not in scores:
                signals.append(self._make_sell(code, current_date, f"模式切换{mode}"))

        top = sorted(scores, key=scores.__getitem__, reverse=True)[: self.top_n]
        weight = 1.0 / max(len(top), 1)
        for code in top:
            if code not in positions:
                signals.append(self._make_buy(code, scores[code], weight,
                                              current_date, f"{mode}模式"))
        return signals


# ============================================================================
# 6. SentimentReversalStrategy
# ============================================================================

class SentimentReversalStrategy(BaseStrategy):
    """情绪反转: 超卖买入，超涨卖出"""
    name = "SentimentReversal"

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)
        self.top_n   = self.config.get("top_n", 10)
        self.oversold_z = self.config.get("oversold_z", -1.5)
        self.overbought_z = self.config.get("overbought_z", 1.5)

    def generate_signals(self, universe, market_data, factor_data,
                         current_date, positions) -> List[Signal]:
        signals = []
        scores = {}

        for code in universe:
            fd = factor_data.get(code)
            if fd is None or fd.empty:
                continue
            rs = fd.get("rsrs_zscore", pd.Series()).dropna()
            if rs.empty:
                continue
            z = float(rs.iloc[-1])
            if z < self.oversold_z:
                scores[code] = -z   # 越超卖越高分

        for code in list(positions.keys()):
            fd = factor_data.get(code)
            if fd is not None and not fd.empty:
                rs = fd.get("rsrs_zscore", pd.Series()).dropna()
                if not rs.empty and float(rs.iloc[-1]) > self.overbought_z:
                    signals.append(self._make_sell(code, current_date, "超买退出"))

        top = sorted(scores, key=scores.__getitem__, reverse=True)[: self.top_n]
        weight = 1.0 / max(len(top), 1)
        for code in top:
            if code not in positions:
                signals.append(self._make_buy(code, scores[code], weight,
                                              current_date, "超卖反转"))
        return signals


# ============================================================================
# 7. KunpengV10Strategy — 微结构策略
# ============================================================================

class KunpengV10Strategy(BaseStrategy):
    """
    鲲鹏V10策略 — 市场微结构因子
    三核心因子:
      SmartMoney  = 大单净流入占比（用 (close-low)/(high-low) * vol 近似）
      StableIlliq = Amihud 非流动性稳定性（低波动非流动 > 稳定持有者存在）
      GapPenalty  = 跳空缺口惩罚（跳空过大降权）
    宽度熔断: 若涨停数/跌停数异常则暂停买入
    """
    name = "KunpengV10"

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)
        self.top_n           = self.config.get("top_n", 15)
        self.illiq_window    = self.config.get("illiq_window", 20)
        self.smart_window    = self.config.get("smart_window", 10)
        self.breadth_limit   = self.config.get("breadth_limit", 0.15)  # 宽度熔断阈值

    def _smart_money(self, df: pd.DataFrame) -> float:
        if not all(c in df.columns for c in ["high", "low", "close", "volume"]):
            return 0.0
        w = self.smart_window
        sub = df.tail(w)
        hl  = (sub["high"] - sub["low"]).replace(0, np.nan)
        buy_vol = (sub["close"] - sub["low"]) / hl * sub["volume"]
        sell_vol = (sub["high"] - sub["close"]) / hl * sub["volume"]
        total_vol = sub["volume"].sum()
        if total_vol < 1:
            return 0.0
        return float((buy_vol.sum() - sell_vol.sum()) / total_vol)

    def _amihud_stable(self, df: pd.DataFrame) -> float:
        if not all(c in df.columns for c in ["close", "volume", "amount"]):
            return 0.0
        sub = df.tail(self.illiq_window).copy()
        ret = sub["close"].pct_change().abs()
        amt = sub["amount"].replace(0, np.nan)
        illiq = (ret / amt).dropna()
        if len(illiq) < 5:
            return 0.0
        return float(-illiq.std())  # 稳定=低波动=高分

    def _gap_penalty(self, df: pd.DataFrame) -> float:
        if "open" not in df.columns or "close" not in df.columns or len(df) < 2:
            return 0.0
        gap = abs(float(df["open"].iloc[-1]) - float(df["close"].iloc[-2]))
        ref = float(df["close"].iloc[-2]) if df["close"].iloc[-2] > 0 else 1
        gap_pct = gap / ref
        return -min(gap_pct, 0.1) * 10   # 最大惩罚 -1.0

    def _breadth_check(self, market_data: Dict) -> bool:
        """宽度熔断：涨跌停比例超限返回 True（需暂停买入）"""
        limit_up = limit_dn = total = 0
        for code, df in market_data.items():
            if "close" not in df.columns or "open" not in df.columns or len(df) < 2:
                continue
            chg = float(df["close"].iloc[-1]) / float(df["close"].iloc[-2]) - 1
            total += 1
            if chg >= 0.095:
                limit_up += 1
            elif chg <= -0.095:
                limit_dn += 1
        if total == 0:
            return False
        return (limit_dn / total) > self.breadth_limit

    def generate_signals(self, universe, market_data, factor_data,
                         current_date, positions) -> List[Signal]:
        signals = []

        # 宽度熔断检测
        if self._breadth_check(market_data):
            logger.info(f"KunpengV10 宽度熔断触发 {current_date}，暂停买入")
            # 仍可卖出
            for code in list(positions.keys()):
                df = market_data.get(code)
                if df is not None and len(df) >= 2:
                    chg = float(df["close"].iloc[-1]) / float(df["close"].iloc[-2]) - 1
                    if chg <= -0.09:
                        signals.append(self._make_sell(code, current_date, "宽度熔断卖出"))
            return signals

        scores = {}
        for code in universe:
            df = market_data.get(code)
            if df is None or len(df) < self.illiq_window:
                continue
            sm  = self._smart_money(df)
            asi = self._amihud_stable(df)
            gp  = self._gap_penalty(df)
            scores[code] = 0.5 * sm + 0.3 * asi + 0.2 * gp

        for code in list(positions.keys()):
            if code not in scores or scores[code] < -0.3:
                signals.append(self._make_sell(code, current_date, "微结构退化"))

        top = sorted(scores, key=scores.__getitem__, reverse=True)[: self.top_n]
        weight = 1.0 / max(len(top), 1)
        for code in top:
            if code not in positions and scores[code] > 0.1:
                signals.append(self._make_buy(code, scores[code], weight,
                                              current_date, f"微结构{scores[code]:.2f}"))
        return signals


# ============================================================================
# 8. AlphaMaxV5FixedStrategy — 机构多因子
# ============================================================================

class AlphaMaxV5FixedStrategy(BaseStrategy):
    """
    AlphaMax V5 (Fixed) — 机构级多因子策略
    七大因子:
      EP          = 盈利收益率 (1/PE_TTM)
      Growth      = 净利润同比增速
      Momentum    = 20日价格动量
      Quality     = ROE_TTM
      Reversal    = 短期反转 (-5日收益)
      Liquidity   = 非流动性 (Amihud)
      ResidualVol = 残差波动率（特质风险）
    特性: 行业中性 + 风险平价权重
    """
    name = "AlphaMaxV5Fixed"

    def __init__(self, config: Optional[Dict] = None) -> None:
        super().__init__(config)
        self.top_n       = self.config.get("top_n", 20)
        self.ep_weight   = self.config.get("ep_weight",   0.20)
        self.growth_w    = self.config.get("growth_w",    0.15)
        self.mom_w       = self.config.get("mom_w",       0.15)
        self.quality_w   = self.config.get("quality_w",  0.20)
        self.rev_w       = self.config.get("rev_w",       0.10)
        self.liq_w       = self.config.get("liq_w",       0.10)
        self.res_vol_w   = self.config.get("res_vol_w",  0.10)

    def _compute_ep(self, fundamental: Optional[Dict]) -> float:
        if not fundamental:
            return 0.0
        pe = fundamental.get("pe_ttm")
        if pe and abs(pe) > 1e-6:
            return 1.0 / pe
        return 0.0

    def _compute_resid_vol(self, df: pd.DataFrame, market_df: Optional[pd.DataFrame]) -> float:
        """残差波动率（特质风险）= std(股票日收益 - beta*市场日收益)"""
        if "close" not in df.columns or len(df) < 20:
            return 0.0
        ret = df["close"].pct_change().tail(60).dropna()
        if market_df is not None and "close" in market_df.columns:
            mkt = market_df["close"].pct_change().reindex(ret.index).dropna()
            common = ret.reindex(mkt.index).dropna()
            mkt = mkt.reindex(common.index)
            if len(common) > 10:
                cov = np.cov(common, mkt)
                beta = cov[0, 1] / (cov[1, 1] + 1e-9) if cov[1, 1] > 1e-9 else 1.0
                resid = common - beta * mkt
                return float(-resid.std())   # 低残差波动=高分
        return float(-ret.std())

    def _zscore_cross_section(self, scores: Dict[str, Dict]) -> Dict[str, float]:
        """截面Z-score + 加权合成"""
        if not scores:
            return {}
        df = pd.DataFrame(scores).T.astype(float)
        for col in df.columns:
            s = df[col]
            std = s.std()
            df[col] = (s - s.mean()) / (std + 1e-9) if std > 1e-9 else 0.0

        weights = {
            "ep": self.ep_weight, "growth": self.growth_w,
            "mom": self.mom_w, "quality": self.quality_w,
            "rev": self.rev_w, "liq": self.liq_w, "resvol": self.res_vol_w,
        }
        total_w = sum(weights.values())
        composite = {}
        for code in df.index:
            s = sum(df.loc[code].get(fn, 0.0) * w for fn, w in weights.items())
            composite[code] = s / total_w
        return composite

    def generate_signals(self, universe, market_data, factor_data,
                         current_date, positions,
                         fundamental_data: Optional[Dict] = None,
                         index_df: Optional[pd.DataFrame] = None) -> List[Signal]:
        signals = []
        raw_scores: Dict[str, Dict] = {}

        for code in universe:
            df  = market_data.get(code)
            fd  = factor_data.get(code)
            fun = (fundamental_data or {}).get(code)

            if df is None or len(df) < 20:
                continue

            close = df["close"]
            pct   = close.pct_change()

            ep      = self._compute_ep(fun)
            growth  = float(fun.get("net_profit_growth", 0.0) or 0.0) if fun else 0.0
            mom     = float(pct.tail(20).add(1).prod() - 1) if len(pct) >= 20 else 0.0
            quality = float(fun.get("roe_ttm", 0.0) or 0.0) if fun else 0.0
            rev     = -float(pct.tail(5).sum()) if len(pct) >= 5 else 0.0

            # Amihud 非流动性（负向，越低越好）
            if "amount" in df.columns:
                amt = df["amount"].tail(20).replace(0, np.nan)
                liq = float(-(pct.tail(20).abs() / amt).mean()) if not amt.isna().all() else 0.0
            else:
                liq = 0.0

            resvol = self._compute_resid_vol(df, index_df)

            raw_scores[code] = {
                "ep": ep, "growth": growth, "mom": mom,
                "quality": quality, "rev": rev, "liq": liq, "resvol": resvol,
            }

        # 截面标准化 + 加权
        composite = self._zscore_cross_section(raw_scores)

        # 退出低分持仓
        for code in list(positions.keys()):
            if composite.get(code, -999) < -0.5:
                signals.append(self._make_sell(code, current_date, "多因子综合分偏低"))

        # 买入 Top-N（风险平价权重需外部传入波动率，此处简化等权）
        top = sorted(composite, key=composite.__getitem__, reverse=True)[: self.top_n]
        weight = 1.0 / max(len(top), 1)
        for code in top:
            if code not in positions and composite[code] > 0.3:
                signals.append(self._make_buy(code, composite[code], weight,
                                              current_date, f"AlphaMax多因子{composite[code]:.2f}"))
        return signals


# ============================================================================
# 策略注册表
# ============================================================================

STRATEGY_REGISTRY: Dict[str, type] = {
    "rsrs_momentum":       RSRSMomentumStrategy,
    "alpha_hunter":        AlphaHunterStrategy,
    "rsrs_advanced":       RSRSAdvancedStrategy,
    "short_term":          ShortTermStrategy,
    "momentum_reversal":   MomentumReversalStrategy,
    "sentiment_reversal":  SentimentReversalStrategy,
    "kunpeng_v10":         KunpengV10Strategy,
    "alpha_max_v5_fixed":  AlphaMaxV5FixedStrategy,
}


def create_strategy(name: str, config: Optional[Dict] = None) -> BaseStrategy:
    cls = STRATEGY_REGISTRY.get(name)
    if cls is None:
        raise ValueError(f"未知策略: {name}，可用: {list(STRATEGY_REGISTRY.keys())}")
    return cls(config)
