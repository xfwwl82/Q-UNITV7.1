# Q-UNITY-V7.1-fixed — 采集性能全面修复版

## 修复内容

| 问题 | 修复 | 预期效果 |
|---|---|---|
| **P0**: 接口断裂 (`enable_akshare` 缺失) | `StockDataPipeline.__init__` 新增 `enable_akshare` + `enrich_akshare()` | 程序可正常启动 |
| **P1**: AKShare 阻塞 TDX | `run()` 中 `enable_akshare=False` 跳过 Phase 1 | TDX 直接开始 |
| **P2**: ThreadPool → ProcessPool | `multiprocessing.Pool + imap_unordered` | 真正并行，无 GIL |
| **P3**: per-request sleep | 每 50 只 sleep 1~3s（per-batch） | 节省 ~315s sleep |

## 预期性能

| 操作 | 修复前 | 修复后 |
|---|---|---|
| 5000只全量采集 | 5+ 小时（AKShare 阻塞） | **~15~25 分钟** |
| TDX 纯 sleep 消耗 | 375s | ~60s |
| 实际并发能力 | 8线程受 GIL 限制 | 8进程真正并行 |

## 新增文件

- `src/data/collector/tdx_process_worker.py` — TDX 多进程工作函数
  - `_tdx_worker_init()`: 进程初始化，建立持久 TDX 连接
  - `_tdx_fetch_worker()`: 进程工作函数，纯数据拉取无 sleep

## 工作流

```bash
# 步骤1：TDX 快速全量（~20分钟）
python main_op.py --workers 8

# 步骤2（可选）：AKShare 扩展字段（~2~4小时，可后台）
python main_op.py --enrich-akshare --ak-workers 2

# 增量更新（日常使用）
python main_op.py

# 补采失败股票
python main_op.py --retry-failed

# 节点测试
python main_op.py --nodes

# 菜单模式
python menu_main.py
```

## 参数调优

```bash
# 控制 per-batch sleep（默认每50只 sleep 1~3s）
python main_op.py --batch-sleep-every 50 --batch-sleep-min 1.0 --batch-sleep-max 3.0

# 如果遇到 TDX 限流，增大 sleep
python main_op.py --batch-sleep-every 30 --batch-sleep-min 2.0 --batch-sleep-max 5.0
```

## 注意事项

- `multiprocessing.Pool` 在 Windows 下需要 `if __name__ == "__main__":` 保护
  - `main_op.py` 和 `menu_main.py` 已包含此保护
  - 直接调用 `pipeline.run()` 时，请确保在 main 守卫内调用
- `spawn` 上下文（Windows/macOS 兼容）已默认启用
- `_tdx_worker_init` 和 `_tdx_fetch_worker` 必须为顶层函数（可 pickle）
