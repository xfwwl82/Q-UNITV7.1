#!/usr/bin/env python3
"""Q-UNITY-V7.1 系统健康检查"""
from __future__ import annotations
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("Q-UNITY-V7.1")


def check_dependencies() -> dict:
    results = {}
    deps = {
        "numpy":    "import numpy; results['numpy']=numpy.__version__",
        "pandas":   "import pandas; results['pandas']=pandas.__version__",
        "scipy":    "import scipy; results['scipy']=scipy.__version__",
        "sklearn":  "import sklearn; results['sklearn']=sklearn.__version__",
        "pyarrow":  "import pyarrow; results['pyarrow']=pyarrow.__version__",
        "akshare":  "import akshare; results['akshare']=akshare.__version__",
        "baostock": "import baostock; results['baostock']='ok'",
        "pytdx":    "from pytdx.hq import TdxHq_API; results['pytdx']='ok'",
        "numba":    "import numba; results['numba']=numba.__version__",
        "tqdm":     "import tqdm; results['tqdm']=tqdm.__version__",
    }
    for pkg, code in deps.items():
        try:
            exec(code, {"results": results})
        except ImportError:
            results[pkg] = "MISSING"
        except Exception as e:
            results[pkg] = f"ERROR: {e}"
    return results


def check_data_dirs() -> dict:
    dirs = {
        "data/parquet": False, "data/cache": False,
        "data/cache/fundamental": False, "data/industry": False,
        "data/reports": False, "logs": False,
    }
    for d in dirs:
        p = Path(d)
        p.mkdir(parents=True, exist_ok=True)
        dirs[d] = p.exists()
    return dirs


def check_config() -> dict:
    path = Path("config.json")
    if not path.exists():
        return {"status": "MISSING"}
    try:
        cfg = json.loads(path.read_text(encoding="utf-8"))
        return {"status": "OK", "keys": list(cfg.keys())}
    except Exception as e:
        return {"status": f"ERROR: {e}"}


def run_health_check() -> bool:
    print("=" * 60)
    print("  Q-UNITY-V7.1 系统健康检查")
    print("=" * 60)

    print("[1] 依赖包状态:")
    deps = check_dependencies()
    required = {"numpy", "pandas", "scipy"}
    all_ok = True
    for pkg, ver in sorted(deps.items()):
        status = "✓" if ver not in ("MISSING",) and not str(ver).startswith("ERROR") else "✗"
        tag = "[必需]" if pkg in required else "[可选]"
        print(f"  {status} {tag} {pkg:12s}: {ver}")
        if pkg in required and status == "✗":
            all_ok = False

    print("[2] 数据目录:")
    dirs = check_data_dirs()
    for d, ok in dirs.items():
        print(f"  {'✓' if ok else '✗'} {d}")

    print("[3] 配置文件:")
    cfg = check_config()
    print(f"  状态: {cfg.get('status')}")
    if "keys" in cfg:
        print(f"  配置项: {cfg['keys']}")
    try:
        with open("config.json", encoding="utf-8") as f:
            raw_cfg = json.load(f)
        if "collector" in raw_cfg:
            c = raw_cfg["collector"]
            print(f"  采集配置: tdx_workers={c.get('tdx_workers','?')} "
                  f"ak_workers={c.get('akshare_workers','?')} "
                  f"adjust={c.get('adjust','?')} ")
    except Exception:
        pass

    print("[4] 核心模块:")
    modules = [
        ("src.types",                    "OrderSide, Signal"),
        ("src.config",                   "ConfigManager"),
        ("src.engine.execution",         "BacktestEngine"),
        ("src.factors.alpha_engine",     "AlphaEngine"),
        ("src.risk.risk_control",        "RiskController"),
        ("src.strategy.strategies",      "STRATEGY_REGISTRY"),
        ("src.data.fundamental",         "FundamentalDataProvider"),
        ("src.data.collector.pipeline",  "StockDataPipeline"),
        ("src.data.collector.validator", "DataValidator"),
        ("src.data.collector.run_report","RunReport"),
    ]
    for mod, items in modules:
        try:
            exec(f"from {mod} import {items}")
            print(f"  ✓ {mod}")
        except Exception as e:
            print(f"  ✗ {mod}: {e}")
            all_ok = False

    print("[5] NB-21 闭环补丁:")
    try:
        from src.factors.alpha_engine import AlphaEngine
        import pandas as pd, numpy as np
        df5 = pd.DataFrame({
            "open": [10.0]*5, "high": [10.5]*5,
            "low": [9.5]*5, "close": [10.0]*5, "volume": [1e6]*5,
        })
        result = AlphaEngine.compute_from_history(df5)
        rsrs_all_nan = result["rsrs_adaptive"].isna().all()
        print(f"  ✓ 5天新股 rsrs_adaptive 全NaN: {rsrs_all_nan}")
        if not rsrs_all_nan:
            print("  ✗ NB-21 补丁未正确屏蔽新股!")
            all_ok = False
    except Exception as e:
        print(f"  ✗ NB-21 验证失败: {e}")
        all_ok = False

    print("" + "=" * 60)
    print(f"  总体状态: {'✅ 健康' if all_ok else '⚠️  存在问题'}")
    print("=" * 60)
    return all_ok


if __name__ == "__main__":
    ok = run_health_check()
    if not ok:
        print("提示: 运行 pip install -r requirements.txt 安装缺失依赖")
    sys.exit(0 if ok else 1)
