# -*- coding: utf-8 -*-
"""
src/realtime/trader.py — 模拟交易引擎 (V7.9)

V7.9 修复内容:
  NF-01 [致命] SimulatedTrader 未实施 A 股 T+1 规则
  NF-02 [致命] 每日最大亏损限制名存实亡（代码从未检查）
  NF-03 [致命] _daily_loss 永不重置（跨日累积）
  NF-07 [严重] SimulatedTrader 仓位计算基于 cash 而非总资产
  NF-10 [严重] 随机滑点方向修正（买入正向，卖出负向）
  NF-16 [低]   open_time 精度问题（新增 entry_date 字段）

SimulatedTrader:
  - 佣金率 0.03%，滑点 0.05%，卖出印花税 0.1%
  - 持仓跟踪: avg_cost / current_price / trailing_wm / holding_days
  - 风控: stop_loss / take_profit / trailing_stop / max_position_pct / max_daily_loss
  - 持久化: JSON 文件 (data/realtime/positions.json)

BrokerAPI: 抽象基类，供接入真实券商 API 时继承
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """NF-16 Fix: 新增 entry_date 字段用于 T+1 判断"""
    code: str
    name: str
    shares: int
    avg_cost: float
    current_price: float
    trailing_wm: float = 0.0      # 追踪止损水位
    holding_days: int = 0
    open_time: float = field(default_factory=time.time)
    # NF-16 Fix: 使用明确的日期字符串，避免时间戳跨日精度问题
    entry_date: str = field(default_factory=lambda: date.today().strftime("%Y-%m-%d"))

    @property
    def market_value(self) -> float:
        return self.shares * self.current_price

    @property
    def cost_value(self) -> float:
        return self.shares * self.avg_cost

    @property
    def pnl(self) -> float:
        return self.market_value - self.cost_value

    @property
    def pnl_pct(self) -> float:
        if self.cost_value == 0:
            return 0.0
        return self.pnl / self.cost_value


@dataclass
class RiskParams:
    stop_loss_pct: float = 0.08
    take_profit_pct: float = 0.20
    trailing_stop_pct: float = 0.05
    max_position_pct: float = 0.10
    max_daily_loss_pct: float = 0.03
    max_positions: int = 10

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "RiskParams":
        risk = config.get("realtime", {}).get("risk", {})
        return cls(
            stop_loss_pct=risk.get("stop_loss_pct", 0.08),
            take_profit_pct=risk.get("take_profit_pct", 0.20),
            trailing_stop_pct=risk.get("trailing_stop_pct", 0.05),
            max_position_pct=risk.get("max_position_pct", 0.10),
            max_daily_loss_pct=risk.get("max_daily_loss_pct", 0.03),
            max_positions=risk.get("max_positions", 10),
        )


class SimulatedTrader:
    """模拟交易引擎 V7.9 - 全量缺陷修复版"""

    COMMISSION_RATE = 0.0003
    SLIPPAGE_RATE = 0.0005
    STAMP_TAX = 0.001

    def __init__(self, config: Optional[Dict[str, Any]] = None,
                 persist_path: str = "data/realtime/positions.json"):
        cfg = config or {}
        rt = cfg.get("realtime", {})
        self.initial_cash: float = rt.get("initial_cash", 1_000_000.0)
        self.cash: float = self.initial_cash
        self.positions: Dict[str, Position] = {}
        self.risk = RiskParams.from_config(cfg)
        self._persist_path = Path(persist_path)
        self._lock = threading.Lock()
        
        # NF-03 Fix: 新增 _daily_loss_date 字段追踪日期
        self._daily_loss: float = 0.0
        self._daily_loss_date: str = date.today().strftime("%Y-%m-%d")
        
        # V7.5: random slippage (True for live simulation, set False for deterministic backtests)
        trading_cfg = cfg.get("realtime", {}).get("trading", {})
        self.enable_random_slippage = bool(trading_cfg.get("enable_random_slippage", True))
        self._load()

    # ------------------------------------------------------------------
    # NF-03 Fix: 跨日重置每日统计
    def reset_daily_stats(self) -> None:
        """在每个交易日开始时调用，重置每日亏损统计"""
        with self._lock:
            self._daily_loss = 0.0
            self._daily_loss_date = date.today().strftime("%Y-%m-%d")
            self._save()
            logger.info("每日亏损统计已重置")

    def _check_and_reset_daily_if_new_day(self) -> None:
        """检查是否跨日，跨日则重置"""
        today = date.today().strftime("%Y-%m-%d")
        if today != self._daily_loss_date:
            self._daily_loss = 0.0
            self._daily_loss_date = today

    # ------------------------------------------------------------------
    # NF-02 Fix: 每日亏损限制检查
    def _check_daily_loss_limit(self) -> bool:
        """检查是否触发每日亏损限制，返回是否允许买入"""
        total_assets = self.cash + sum(p.market_value for p in self.positions.values())
        daily_loss_limit = total_assets * self.risk.max_daily_loss_pct
        if -self._daily_loss >= daily_loss_limit:
            return False
        return True

    # ------------------------------------------------------------------
    def buy(self, code: str, name: str, price: float,
            shares=None):
        with self._lock:
            # NF-03 Fix: 检查并重置跨日统计
            self._check_and_reset_daily_if_new_day()
            
            # NF-02 Fix: 每日亏损限制检查
            if not self._check_daily_loss_limit():
                return {"ok": False, "reason": f"当日亏损限额({self._daily_loss:.2f})已触发"}
            
            # NF-10 Fix: 随机滑点只能为正值（买入价上浮）
            if self.enable_random_slippage:
                slip = abs(random.uniform(0, 1)) * self.SLIPPAGE_RATE
            else:
                slip = self.SLIPPAGE_RATE
            exec_price = price * (1 + slip)
            
            # NF-07 Fix: 仓位计算基于总资产而非现金
            mv = sum(p.market_value for p in self.positions.values())
            total_assets = self.cash + mv
            max_cash = min(
                total_assets * self.risk.max_position_pct,
                self.cash  # 不能超过可用现金
            )
            
            if shares is None:
                shares = int(max_cash / exec_price / 100) * 100
            if shares <= 0:
                return {"ok": False, "reason": "shares=0"}
            cost = exec_price * shares
            commission = max(cost * self.COMMISSION_RATE, 5.0)
            total = cost + commission
            if total > self.cash:
                return {"ok": False, "reason": "cash insufficient"}
            if len(self.positions) >= self.risk.max_positions and code not in self.positions:
                return {"ok": False, "reason": "max_positions reached"}

            self.cash -= total
            if code in self.positions:
                p = self.positions[code]
                new_shares = p.shares + shares
                p.avg_cost = (p.avg_cost * p.shares + exec_price * shares) / new_shares
                p.shares = new_shares
                p.current_price = price
                p.trailing_wm = max(p.trailing_wm, price)
                # NF-16 Fix: 更新 entry_date 为当日
                p.entry_date = date.today().strftime("%Y-%m-%d")
            else:
                self.positions[code] = Position(
                    code=code, name=name, shares=shares,
                    avg_cost=exec_price, current_price=price,
                    trailing_wm=price,
                    entry_date=date.today().strftime("%Y-%m-%d")  # NF-16 Fix
                )
            self._save()
            return {"ok": True, "shares": shares, "exec_price": exec_price,
                    "commission": commission, "cash_left": self.cash}

    def sell(self, code: str, price: float,
             shares=None):
        with self._lock:
            # NF-03 Fix: 检查并重置跨日统计
            self._check_and_reset_daily_if_new_day()
            
            if code not in self.positions:
                return {"ok": False, "reason": "position not found"}
            
            p = self.positions[code]
            
            # NF-01 Fix: T+1 校验 - 买入日期必须早于今日
            buy_date_str = p.entry_date
            today_str = date.today().strftime("%Y-%m-%d")
            if buy_date_str >= today_str:
                return {"ok": False, "reason": f"T+1限制: 买入日={buy_date_str}, 今日={today_str}"}
            
            # NF-10 Fix: 随机滑点只能为负值（卖出价下浮）
            if self.enable_random_slippage:
                slip = abs(random.uniform(0, 1)) * self.SLIPPAGE_RATE
            else:
                slip = self.SLIPPAGE_RATE
            exec_price = price * (1 - slip)
            
            if shares is None or shares >= p.shares:
                shares = p.shares
            proceeds = exec_price * shares
            commission = max(proceeds * self.COMMISSION_RATE, 5.0)
            stamp = proceeds * self.STAMP_TAX
            net = proceeds - commission - stamp
            pnl = net - p.avg_cost * shares
            
            # NF-03 Fix: 累计每日亏损
            self._daily_loss += min(pnl, 0)
            
            self.cash += net
            if shares >= p.shares:
                del self.positions[code]
            else:
                p.shares -= shares
            self._save()
            return {"ok": True, "shares": shares, "exec_price": exec_price,
                    "pnl": pnl, "net_proceeds": net, "cash_left": self.cash}

    def update_prices(self, prices: Dict[str, float]) -> List[Dict[str, Any]]:
        """更新持仓价格，返回触发风控的信号列表"""
        triggered = []
        with self._lock:
            for code, price in prices.items():
                if code not in self.positions:
                    continue
                p = self.positions[code]
                p.current_price = price
                p.trailing_wm = max(p.trailing_wm, price)

                # 止损
                if p.pnl_pct <= -self.risk.stop_loss_pct:
                    triggered.append({"code": code, "event": "stop_loss",
                                      "pnl_pct": p.pnl_pct, "price": price})
                # 止盈
                elif p.pnl_pct >= self.risk.take_profit_pct:
                    triggered.append({"code": code, "event": "take_profit",
                                      "pnl_pct": p.pnl_pct, "price": price})
                # 追踪止损
                elif p.trailing_wm > 0:
                    drop = (p.trailing_wm - price) / p.trailing_wm
                    if drop >= self.risk.trailing_stop_pct:
                        triggered.append({"code": code, "event": "trailing_stop",
                                          "pnl_pct": p.pnl_pct, "price": price})

            if triggered:
                self._save()
        return triggered

    def get_positions(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [asdict(p) for p in self.positions.values()]

    def get_account_summary(self) -> Dict[str, Any]:
        with self._lock:
            mv = sum(p.market_value for p in self.positions.values())
            total = self.cash + mv
            return {
                "cash": self.cash,
                "market_value": mv,
                "total_assets": total,
                "pnl": total - self.initial_cash,
                "pnl_pct": (total - self.initial_cash) / self.initial_cash,
                "position_count": len(self.positions),
                "daily_loss": self._daily_loss,
                "daily_loss_date": self._daily_loss_date,
            }

    # ------------------------------------------------------------------
    def _save(self) -> None:
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "cash": self.cash,
            "initial_cash": self.initial_cash,
            "daily_loss": self._daily_loss,
            "daily_loss_date": self._daily_loss_date,  # NF-16 Fix
            "positions": {k: asdict(v) for k, v in self.positions.items()},
        }
        self._persist_path.write_text(json.dumps(data, ensure_ascii=False, indent=2),
                                      encoding="utf-8")

    def _load(self) -> None:
        if not self._persist_path.exists():
            return
        try:
            data = json.loads(self._persist_path.read_text(encoding="utf-8"))
            self.cash = data.get("cash", self.initial_cash)
            self.initial_cash = data.get("initial_cash", self.initial_cash)
            self._daily_loss = data.get("daily_loss", 0.0)
            # NF-16 Fix: 使用 setdefault 防止旧JSON缺少字段
            self._daily_loss_date = data.get("daily_loss_date", "1970-01-01")
            for k, v in data.get("positions", {}).items():
                # NF-16 Fix: 旧数据缺少 entry_date 时使用默认值
                v.setdefault("entry_date", "1970-01-01")
                self.positions[k] = Position(**v)
            logger.info("Loaded %d positions from %s", len(self.positions), self._persist_path)
        except Exception as e:
            logger.warning("Failed to load positions: %s", e)


class BrokerAPI(ABC):
    """真实券商 API 抽象基类"""

    @abstractmethod
    def place_order(self, code: str, direction: str, price: float, shares: int) -> str:
        """下单, 返回 order_id"""

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        """撤单"""

    @abstractmethod
    def get_position(self, code: str) -> Dict[str, Any]:
        """查询持仓"""

    @abstractmethod
    def get_account(self) -> Dict[str, Any]:
        """查询账户"""
