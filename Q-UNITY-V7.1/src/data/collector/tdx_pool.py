#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tdx_pool.py — TDX 线程安全连接池 (threading.local)
====================================================
pytdx 的 TdxHq_API 不是线程安全的。
本模块通过 threading.local() 保证每线程独立 socket 连接。
"""

import random
import threading
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

try:
    from pytdx.hq import TdxHq_API
    from pytdx.params import TDXParams
    _PYTDX_AVAILABLE = True
except ImportError:
    TdxHq_API = None   # type: ignore[assignment,misc]
    TDXParams  = None  # type: ignore[assignment]
    _PYTDX_AVAILABLE = False


class TDXConnectionPool:
    """
    线程安全 TDX 连接池。
    每线程通过 get_connection() 获取专属 TdxHq_API，长连接复用。
    """

    def __init__(self, top_nodes: List[Dict], timeout: float = 10.0, max_retry: int = 3) -> None:
        if not _PYTDX_AVAILABLE:
            raise ImportError("pytdx 未安装，请: pip install pytdx")
        if not top_nodes:
            raise ValueError("top_nodes 不能为空")
        self.top_nodes = top_nodes
        self.timeout   = timeout
        self.max_retry = max_retry
        self._local    = threading.local()
        logger.info("TDXConnectionPool: %d 节点，超时=%.1fs", len(top_nodes), timeout)

    def _create_connection(self) -> Optional["TdxHq_API"]:
        top_pool = self.top_nodes[:3].copy()
        random.shuffle(top_pool)
        candidate_order = top_pool + self.top_nodes[3:]
        attempts = min(self.max_retry, len(candidate_order))
        for i in range(attempts):
            node = candidate_order[i]
            api  = TdxHq_API(heartbeat=True, auto_retry=True)
            try:
                api.connect(node["host"], node["port"], time_out=self.timeout)
                logger.debug("[TID-%s] 连接 → %s", threading.get_ident(), node["name"])
                return api
            except Exception as exc:
                logger.warning("[TID-%s] 连接失败 %s: %s", threading.get_ident(), node["name"], exc)
                try:
                    api.disconnect()
                except Exception:
                    pass
        logger.error("[TID-%s] 所有节点连接失败", threading.get_ident())
        return None

    def _connect_best(self) -> Optional["TdxHq_API"]:
        """对外统一入口：从最优节点池重新建立连接并更新线程本地存储。"""
        api = self._create_connection()
        self._local.api = api
        return api

    def get_connection(self) -> Optional["TdxHq_API"]:
        """获取/复用当前线程的 TdxHq_API 连接（含探活自愈心跳）。"""
        if hasattr(self._local, "api") and self._local.api is not None:
            # 探活：执行最轻量请求验证连接存活
            try:
                self._local.api.get_security_count(0)
                return self._local.api
            except Exception:
                # 长连接因网络波动假死，触发自愈重连
                logger.info("[TID-%s] 连接失效，正在重连", threading.get_ident())
                self._safe_disconnect(self._local.api)
                self._local.api = None
                return self._connect_best()
        # 线程首次获取连接
        return self._connect_best()

    def release(self) -> None:
        api = getattr(self._local, "api", None)
        if api is not None:
            self._safe_disconnect(api)
            self._local.api = None

    def close_all(self) -> None:
        self.release()

    @staticmethod
    def _safe_disconnect(api: "TdxHq_API") -> None:
        try:
            api.disconnect()
        except Exception:
            pass

    def __enter__(self) -> Optional["TdxHq_API"]:
        return self.get_connection()

    def __exit__(self, *_) -> None:
        pass


_global_pool: Optional[TDXConnectionPool] = None
_pool_lock = threading.Lock()


def get_global_pool(top_nodes: Optional[List[Dict]] = None, timeout: float = 10.0) -> TDXConnectionPool:
    global _global_pool
    if _global_pool is None:
        with _pool_lock:
            if _global_pool is None:
                if top_nodes is None:
                    raise ValueError("首次调用必须提供 top_nodes")
                _global_pool = TDXConnectionPool(top_nodes, timeout=timeout)
    return _global_pool


def reset_global_pool() -> None:
    global _global_pool
    with _pool_lock:
        if _global_pool is not None:
            _global_pool.close_all()
        _global_pool = None
