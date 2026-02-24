# Q-UNITY-V6-op patch_v9 — 双轨并行 A 股日线采集引擎（全程 HFQ）

## 架构变更摘要 (v8 → v9)

### v7（串行降级 — 已废弃）
```
TDX ─成功─→ 写盘（缺 turnover）
     └失败─→ AKShare（线程不安全！）─成功─→ 写盘
                            └失败─→ BaoStock
```

### v8（双轨并行 raw+hfq — 已升级）
```
轨道A: TDX   (8线程)  ─→ OHLCV (raw, 未复权)
轨道B: AKShare (2进程) ─→ 扩展字段 (hfq) → LEFT JOIN → adjust=partial_hfq
Fallback: BaoStock (hfq)
```

### v9（双轨并行 HFQ — 当前）
```
轨道A: TDX   (8线程, ThreadPool)  ─→ OHLCV (hfq adjustflag=2, 高速)
                                                ↓
轨道B: AKShare (2进程, ProcessPool) ─→ 扩展字段: turnover/pct_change (hfq)
                                                ↓
                                   date 为键 LEFT JOIN 合并（全程 hfq）
                                                ↓
Fallback: 两轨均失败 → BaoStock (hfq, 完整字段)
                                                ↓
                             DataValidator 三层验证（行数/列/OHLC）
                                                ↓
                             流式写盘 + RunReport 持久化
```

## 关键设计决策

| 问题 | v7 | v8 | v9 |
|---|---|---|---|
| AKShare 线程安全 | ❌ ThreadPool 共享 session | ✅ ProcessPool 进程隔离 | ✅ 同 v8 |
| AKShare 限流重试 | ❌ 无 | ✅ 30/60/90s 限流退避 | ✅ 同 v8 |
| turnover 字段 | ❌ 仅 TDX 成功时永远丢失 | ✅ AKShare 独立轨道专采 | ✅ 同 v8 |
| 复权统一 | ❌ 三源不统一 | ⚠️ AKShare/BS=hfq, TDX=raw | ✅ 三源全部 hfq |
| TDX HFQ | ❌ 无 | ❌ 无（原始价） | ✅ adjustflag=2，老节点自动降级告警 |
| TDX 连接探活 | ❌ 无 | ⚠️ logger.debug 静默重建 | ✅ logger.info + _connect_best() 自愈 |
| 数据验证 | ❌ 无 | ✅ 写盘前三层验证 | ✅ 同 v8 |
| 失败持久化 | ❌ 内存计数 | ✅ failed_stocks.txt | ✅ 同 v8 |
| 补采机制 | ❌ 无 | ✅ --retry-failed | ✅ 同 v8 |
| 流式写盘 | ❌ 全量后批量 | ✅ 每股完成立即写 | ✅ 同 v8 |

## 模块结构
```
src/data/collector/
├── node_scanner.py    # 24节点赛马（ThreadPool 并发探针）
├── tdx_pool.py        # 线程安全连接池（threading.local）
├── akshare_client.py  # 进程隔离采集（ProcessPool + 限流感知退避）
├── baostock_client.py # BaoStock 兜底（hfq, 独立 login/logout）
├── incremental.py     # 智能增量更新（max_date + merge + dedup）
├── validator.py       # DataValidator（行数/列/OHLC 三层验证）
├── run_report.py      # RunReport（failed_stocks.txt + JSON 审计）
└── pipeline.py        # 双轨并行主引擎（TDX线程 + AKShare进程 + 合并）
```

## 快速开始
```bash
pip install pandas numpy pyarrow pytdx baostock akshare tqdm

# 节点赛马
python main_op.py --nodes

# 采集 20 只测试
python main_op.py --sample 20

# 全量采集
python main_op.py --workers 8 --ak-workers 2

# 补采失败股票
python main_op.py --retry-failed

# 强制全量重下载
python main_op.py --full

# 单元测试（T1~T8）
pytest tests/test_collector.py -v
```

## 输出文件说明
```
data/
├── parquet/
│   ├── 000001.parquet    # 含 turnover/pct_change 扩展字段
│   └── ...
└── reports/
    ├── failed_stocks.txt          # 失败列表（用于 --retry-failed）
    └── run_stats_20260223_120000.json  # 完整审计日志
```

## 复权说明
- **TDX**：`adjust="hfq"`（后复权，通过 `adjustflag=2` 参数请求）；老节点不支持时自动降级并打印 WARNING
- **AKShare**：`adjust="hfq"`（后复权），用于扩展字段（turnover 等）  
- **BaoStock**：`adjust="hfq"`（后复权），兜底完整数据
- **合并后**：`adjust="hfq"`（三源复权体系完全统一，OHLCV + 扩展字段均为后复权）
- 相比 patch_v8，patch_v9 消除了 TDX 数据与 AKShare/BaoStock 数据间的复权不一致问题
