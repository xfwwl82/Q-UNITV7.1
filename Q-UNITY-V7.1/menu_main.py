#!/usr/bin/env python3
"""
Q-UNITY-V7.1 主菜单系统

【V7.1 数据管理修复说明】
原 op-v3 阶段顺序缺陷:
  AKShare全量(5190只×7s/2进程 = 5+小时) → TDX（没有速度！pytdx 根本未被调用）

V7.1 修复:
  选项1: TDX 快速全量采集  — 仅 TDX 后复权，8线程，~30分钟，立即可用
  选项2: AKShare 扩展字段  — 对已采集数据补充 turnover/pct_change（独立步骤）
  选项3: 增量更新          — TDX 快速增量
  选项4: 补采失败股票
"""
from __future__ import annotations
import json
import logging
import socket
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)


def _check_data_source_heartbeat() -> None:
    print("" + "─" * 50)
    print("  数据源心跳检测")
    print("─" * 50)

    print("[1] AKShare:")
    try:
        import akshare as ak
        t0 = time.time()
        df = ak.stock_zh_index_spot_em(symbol="上证指数")
        elapsed = time.time() - t0
        if df is not None and not df.empty:
            print(f"  ✓ 连通正常 ({elapsed:.2f}s)")
        else:
            print(f"  ✗ 返回空数据 ({elapsed:.2f}s)")
    except ImportError:
        print("  ✗ akshare 未安装")
    except Exception as e:
        print(f"  ✗ 异常: {e}")

    print("[2] TDX 节点（前5优选）:")
    try:
        from src.data.collector.node_scanner import get_fastest_nodes
        nodes = get_fastest_nodes(top_n=5, timeout=3.0)
        for n in nodes:
            icon = "✓" if n["status"] == "ok" else "✗"
            lat  = f"{n['latency_ms']:.0f}ms" if n["latency_ms"] >= 0 else "timeout"
            print(f"  {icon} {n['name']} {n['host']}:{n['port']}  {lat}")
    except Exception as e:
        print(f"  ✗ 节点扫描失败: {e}")

    print("[3] BaoStock:")
    try:
        import baostock as bs
        t0 = time.time()
        lg = bs.login()
        elapsed = time.time() - t0
        if lg.error_code == "0":
            print(f"  ✓ 登录成功 ({elapsed:.2f}s)")
            bs.logout()
        else:
            print(f"  ✗ 登录失败: {lg.error_msg}")
    except ImportError:
        print("  ✗ baostock 未安装")
    except Exception as e:
        print(f"  ✗ 异常: {e}")

    print("[4] Tushare (DNS):")
    try:
        t0 = time.time()
        ip = socket.gethostbyname("api.tushare.pro")
        elapsed = time.time() - t0
        print(f"  ✓ DNS 解析正常 → {ip} ({elapsed*1000:.0f}ms)")
    except socket.gaierror as e:
        print(f"  ✗ DNS 解析失败: {e}")

    print("" + "─" * 50)
    input("按 Enter 返回...")


def _load_collector_config() -> dict:
    defaults = {
        "parquet_dir":    "./data/parquet",
        "reports_dir":    "./data/reports",
        "top_n_nodes":    5,
        "tdx_workers":    8,
        "ak_workers":     2,
        "ak_delay_min":   0.3,
        "ak_delay_max":   0.8,
        "ak_max_retries": 3,
    }
    try:
        cfg_path = Path("config.json")
        if cfg_path.exists():
            raw = json.loads(cfg_path.read_text(encoding="utf-8"))
            c = raw.get("collector", {})
            d = raw.get("data", {})
            defaults.update({
                "parquet_dir":    d.get("parquet_dir",         defaults["parquet_dir"]),
                "reports_dir":    d.get("reports_dir",         defaults["reports_dir"]),
                "top_n_nodes":    c.get("tdx_top_nodes",       defaults["top_n_nodes"]),
                "tdx_workers":    c.get("tdx_workers",         defaults["tdx_workers"]),
                "ak_workers":     c.get("akshare_workers",     defaults["ak_workers"]),
                "ak_delay_min":   c.get("akshare_delay_min",   defaults["ak_delay_min"]),
                "ak_delay_max":   c.get("akshare_delay_max",   defaults["ak_delay_max"]),
                "ak_max_retries": c.get("akshare_max_retries", defaults["ak_max_retries"]),
            })
    except Exception:
        pass
    return defaults


def _make_pipeline(force_full: bool = False, enable_akshare: bool = False):
    from src.data.collector.pipeline import StockDataPipeline
    cfg = _load_collector_config()
    return StockDataPipeline(
        parquet_dir=cfg["parquet_dir"],
        reports_dir=cfg["reports_dir"],
        top_n_nodes=cfg["top_n_nodes"],
        tdx_workers=cfg["tdx_workers"],
        ak_workers=cfg["ak_workers"],
        ak_delay_min=cfg["ak_delay_min"],
        ak_delay_max=cfg["ak_delay_max"],
        ak_max_retries=cfg["ak_max_retries"],
        force_full=force_full,
        enable_akshare=enable_akshare,
    )


def _print_stats(stats: dict) -> None:
    mode = "双轨(TDX+AKShare)" if stats.get("akshare_enabled") else "快速(仅TDX)"
    print(f"{'='*54}")
    print(f"  采集完成  [{mode}]")
    print(f"{'='*54}")
    print(f"  总计:   {stats.get('total',   0):>6} 只")
    print(f"  成功:   {stats.get('success', 0):>6} 只")
    print(f"  失败:   {stats.get('failed',  0):>6} 只")
    print(f"  跳过:   {stats.get('skipped', 0):>6} 只（已最新）")
    print(f"  耗时:   {stats.get('elapsed_s', 0):>6.1f} 秒")
    print(f"  速度:   {stats.get('speed',    0):>6.1f} 股/秒")
    print(f"  报告:   {stats.get('reports_dir', 'N/A')}")
    print(f"{'='*54}")


def data_management_menu(config=None, storage=None) -> None:
    while True:
        print("" + "═" * 56)
        print("  数据管理  (V7.1 TDX快速 + AKShare可选扩展)")
        print("═" * 56)
        print("  1. TDX 快速全量采集   (~30min，HFQ，无扩展字段)")
        print("  2. AKShare 扩展字段补充 (turnover/pct_change，独立步骤)")
        print("  3. 增量数据更新        (仅补采缺失日期)")
        print("  4. 补采失败股票        (failed_stocks.txt)")
        print("  5. 数据完整性检查")
        print("  6. 节点赛马测试")
        print("  7. 清理缓存")
        print("  0. 返回主菜单")
        print()
        print("  ℹ  完整工作流: 先选1(~30min) → 再选2(可选,~2~4h)")
        choice = input("请选择 [0-7]: ").strip()

        if choice == "0":
            break
        elif choice == "1":
            _cmd_tdx_fast_collect()
        elif choice == "2":
            _cmd_enrich_akshare()
        elif choice == "3":
            _cmd_incremental_update()
        elif choice == "4":
            _cmd_retry_failed()
        elif choice == "5":
            _cmd_check_integrity(storage)
        elif choice == "6":
            _cmd_node_race()
        elif choice == "7":
            _cmd_clean_cache()
        else:
            print("  ✗ 无效选项")


def _cmd_tdx_fast_collect() -> None:
    print("⚡ TDX 快速全量采集（仅 TDX，后复权 HFQ）")
    print("  模式: TDX 8线程并发，不启动 AKShare 进程")
    print("  预计: 5000+ 只 A 股约 20~40 分钟")
    print("  字段: open/high/low/close/vol/amount（不含 turnover/pct_change）")
    print("  后续: 如需扩展字段，完成后选「选项2」单独补充")
    confirm = input("  确认开始? [y/N]: ").strip().lower()
    if confirm != "y":
        print("  已取消。")
        return
    try:
        pipeline = _make_pipeline(force_full=True, enable_akshare=False)
        stats = pipeline.download_all_a_stocks()
        _print_stats(stats)
    except ImportError as e:
        print(f"  ✗ 导入失败: {e}")
    except Exception as e:
        logger.exception("TDX 快速采集异常")
        print(f"  ✗ 失败: {e}")


def _cmd_enrich_akshare() -> None:
    print("🔬 AKShare 扩展字段补充（独立步骤）")
    print("  说明: 对已有 Parquet 文件补充 turnover、pct_change、amplitude 字段")
    print("  模式: 进程隔离（2进程），包含限流感知退避（30/60/90s）")
    print("  预计: 5000只约 2~4 小时（受东方财富接口限流影响）")
    print("  前提: 请先完成「选项1: TDX 快速全量采集」")

    cfg = _load_collector_config()
    parquet_dir = Path(cfg["parquet_dir"])
    existing = list(parquet_dir.glob("*.parquet"))
    if not existing:
        print(f"  ✗ Parquet 目录为空: {parquet_dir}")
        print("  请先执行「选项1: TDX 快速全量采集」。")
        return

    print(f"  当前本地 Parquet: {len(existing)} 只")
    # 快速抽样检查是否已有扩展字段
    import pandas as pd
    has_ext = no_ext = 0
    for f in existing[:30]:
        try:
            cols = pd.read_parquet(f).columns.tolist()
            if "turnover" in cols:
                has_ext += 1
            else:
                no_ext += 1
        except Exception:
            no_ext += 1
    print(f"  抽样（前30只）: {has_ext} 只已有 turnover，{no_ext} 只未含扩展字段")

    confirm = input("  确认开始? [y/N]: ").strip().lower()
    if confirm != "y":
        print("  已取消。")
        return
    try:
        pipeline = _make_pipeline(force_full=False, enable_akshare=True)
        stats = pipeline.enrich_akshare()
        print(f"  ✓ 完成: 处理 {stats.get('total',0)} 只，"
              f"成功 {stats.get('success',0)} 只，失败 {stats.get('failed',0)} 只")
    except ImportError as e:
        print(f"  ✗ 导入失败: {e}")
    except Exception as e:
        logger.exception("AKShare 扩展字段补充异常")
        print(f"  ✗ 失败: {e}")


def _cmd_incremental_update() -> None:
    print("🔄 增量数据更新（TDX，仅补采缺失日期）")
    try:
        pipeline = _make_pipeline(force_full=False, enable_akshare=False)
        stock_list = pipeline._get_all_a_stock_list()
        if not stock_list:
            print("  ✗ 获取股票列表失败")
            return
        print(f"  共 {len(stock_list)} 只 A 股，开始增量更新...")
        stats = pipeline.run(stock_list)
        _print_stats(stats)
    except ImportError as e:
        print(f"  ✗ 导入失败: {e}")
    except Exception as e:
        logger.exception("增量更新异常")
        print(f"  ✗ 失败: {e}")


def _cmd_retry_failed() -> None:
    print("🔁 补采失败股票")
    cfg = _load_collector_config()
    failed_txt = Path(cfg["reports_dir"]) / "failed_stocks.txt"
    if not failed_txt.exists():
        print(f"  ⚠️  未找到失败列表: {failed_txt}")
        print("  请先执行全量采集或增量更新。")
        return
    try:
        pipeline = _make_pipeline(force_full=True, enable_akshare=False)
        stats = pipeline.retry_failed(reports_dir=cfg["reports_dir"])
        _print_stats(stats)
    except Exception as e:
        logger.exception("补采异常")
        print(f"  ✗ 失败: {e}")


def _cmd_check_integrity(storage=None) -> None:
    print("🔍 数据完整性检查")
    cfg = _load_collector_config()
    parquet_dir = Path(cfg["parquet_dir"])
    if not parquet_dir.exists():
        print(f"  ⚠️  Parquet 目录不存在: {parquet_dir}")
        return
    files = list(parquet_dir.glob("*.parquet"))
    print(f"  已存储 {len(files)} 只股票的 Parquet 文件")
    if files:
        import pandas as pd
        print("  抽样检查（前5只）:")
        for f in files[:5]:
            try:
                df = pd.read_parquet(f)
                ext = [c for c in ("turnover", "pct_change", "adjust") if c in df.columns]
                d_min = df["date"].min() if "date" in df.columns else "?"
                d_max = df["date"].max() if "date" in df.columns else "?"
                print(f"    ✓ {f.stem}: {len(df)} 行 | {d_min} ~ {d_max} | 扩展={ext}")
            except Exception as ex:
                print(f"    ✗ {f.stem}: {ex}")
    if storage:
        codes = storage.get_all_codes()
        print(f"  ColumnarStorage: {len(codes)} 只股票")
    input("按 Enter 返回...")


def _cmd_node_race() -> None:
    print("🏁 TDX 节点赛马测试")
    try:
        from src.data.collector.node_scanner import race_nodes, TDX_NODES
        print(f"  测试 {len(TDX_NODES)} 个候选节点...")
        results = race_nodes(timeout=3.0)
        ok_nodes = [r for r in results if r["status"] == "ok"]
        print(f"  结果（{len(ok_nodes)}/{len(results)} 可达）:")
        for i, r in enumerate(results[:10], 1):
            icon = "✓" if r["status"] == "ok" else "✗"
            lat  = f"{r['latency_ms']:>7.2f} ms" if r["latency_ms"] >= 0 else "  timeout"
            print(f"    {i:>2}. {icon} {r['name']:<10} {r['host']:>18}:{r['port']}  {lat}")
        if len(results) > 10:
            print(f"    ... 余 {len(results)-10} 个节点")
    except Exception as e:
        print(f"  ✗ 节点测试失败: {e}")
    input("按 Enter 返回...")


def _cmd_clean_cache() -> None:
    print("🗑️  清理缓存")
    cleaned = 0
    for p in Path("./data/cache").rglob("*.json") if Path("./data/cache").exists() else []:
        try:
            p.unlink()
            cleaned += 1
        except Exception:
            pass
    print(f"  已清理 {cleaned} 个缓存文件（fundamental JSON 缓存）")
    print("  注: Parquet 行情文件和运行报告不会被清除。")


def backtest_menu(config=None) -> None:
    while True:
        print("" + "═" * 40)
        print("  回测系统")
        print("═" * 40)
        print("  1. 运行单策略回测")
        print("  2. 多策略对比")
        print("  3. 查看历史回测结果")
        print("  0. 返回主菜单")
        choice = input("请选择 [0-3]: ").strip()
        if choice == "0":
            break
        elif choice == "1":
            from src.strategy.strategies import STRATEGY_REGISTRY
            print(f"可用策略: {list(STRATEGY_REGISTRY.keys())}")
            name = input("输入策略名称: ").strip()
            if name in STRATEGY_REGISTRY:
                print(f"  已选择: {name}")
            else:
                print(f"  ✗ 未知策略: {name}")
        elif choice == "2":
            print("  多策略对比功能开发中...")
        elif choice == "3":
            results_dir = Path("results")
            if results_dir.exists():
                files = list(results_dir.glob("*.json"))
                print(f"  共 {len(files)} 个历史结果")
                for f in files[:10]:
                    print(f"    {f.name}")
            else:
                print("  暂无历史回测结果")
        else:
            print("  ✗ 无效选项")


def system_management_menu(config=None) -> None:
    while True:
        print("" + "═" * 40)
        print("  系统管理")
        print("═" * 40)
        print("  1. 健康检查")
        print("  2. 查看日志")
        print("  3. 数据源心跳检测")
        print("  0. 返回主菜单")
        choice = input("请选择 [0-3]: ").strip()
        if choice == "0":
            break
        elif choice == "1":
            from main import run_health_check
            run_health_check()
        elif choice == "2":
            log_path = Path("logs/q-unity.log")
            if log_path.exists():
                lines = log_path.read_text(encoding="utf-8").splitlines()
                print(f"最近 20 行日志:")
                for line in lines[-20:]:
                    print(f"  {line}")
            else:
                print("  暂无日志文件")
        elif choice == "3":
            _check_data_source_heartbeat()
        else:
            print("  ✗ 无效选项")


def main_menu() -> None:
    config = storage = None
    try:
        from src.config import ConfigManager
        from src.data.storage import ColumnarStorageManager
        config  = ConfigManager()
        data_dir = config.get("data", {}).get("base_dir", "./data")
        storage = ColumnarStorageManager(data_dir)
    except Exception as e:
        print(f"⚠️  初始化失败: {e}")

    while True:
        print("" + "═" * 56)
        print("       Q-UNITY-V7.1 量化交易系统 v7.1.0")
        print("       数据层: TDX快速(~30min) + AKShare可选扩展")
        print("═" * 56)
        print("  1. 数据管理  (快速采集/扩展字段/增量/节点)")
        print("  2. 回测系统")
        print("  3. 系统管理  (健康检查/日志/心跳)")
        print("  0. 退出")
        print("─" * 56)
        choice = input("请选择 [0-3]: ").strip()
        if choice == "0":
            print("再见! Q-UNITY-V7.1 已退出。")
            sys.exit(0)
        elif choice == "1":
            data_management_menu(config, storage)
        elif choice == "2":
            backtest_menu(config)
        elif choice == "3":
            system_management_menu(config)
        else:
            print("  ✗ 无效选项，请重新选择")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    main_menu()
