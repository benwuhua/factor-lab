# Factor Lab Current Research Workflow

本文梳理当前 factor-lab 从数据、因子、自动挖掘、组合、专家复核到纸面执行的业务流程。

## 1. 数据治理

当前研究数据库固定服务两个 A 股核心股票池：

- 沪深300: `configs/provider_csi300_current.yaml`
- 中证500: `configs/provider_current.yaml`

主要数据入口：

- 行情与 OHLCV: `scripts/build_akshare_qlib_data.py`
- 证券主数据与公告事件: `scripts/build_research_context_data.py`
- 数据质量检查: `scripts/check_data_quality.py`

业务目标是保证研究、回测、组合和风控都在同一套股票池与同一套时点数据上运行。

## 2. 因子研究

因子来源分三类：

- 手工定义因子: `factors/registry.yaml`
- JoinQuant 迁移因子: 本地可表达的量价、换手、情绪、技术类因子
- 自动挖掘表达式: `configs/autoresearch/candidates/example_expression.yaml`

单因子评价入口：

- `scripts/eval_factor.py`
- `scripts/batch_eval_factors.py`
- `scripts/backtest_factor_events.py`

评价指标包括 IC、Rank IC、分层收益、多空收益、换手、样本数，以及事件型因子的分位桶收益。

## 3. 自动挖掘

autoresearch 的核心原则是“agent 只改候选表达式，不改评估器”。

固定边界：

- provider: 沪深300 / 中证500
- horizons: 5 和 20
- purification: 默认 `mad -> zscore`
- neutralization: raw 和 size_proxy neutralized
- ledger: `reports/autoresearch/expression_results.tsv`

运行入口：

```bash
make autoresearch-expression
make autoresearch-codex-loop AUTORESEARCH_CODEX_UNTIL=08:30 AUTORESEARCH_CODEX_ITERATIONS=30
```

每轮实验都会生成 summary block、raw/neutralized eval、candidate copy 和 ledger row。

## 4. 因子净化

新增的净化层参考 AlphaPurify 思路，但保留为 factor-lab 内部轻量实现。

当前支持：

- MAD 去极值
- z-score 标准化
- rank 标准化
- OLS 残差中性化

用途：

- 减少极端值对 IC/分层收益的扰动
- 让不同候选因子进入同一个可比尺度
- 在 autoresearch 中固定净化流程，避免 agent 通过异常值优化结果

## 5. 因子治理与组合生成

通过 `scripts/select_factors.py` 将稳定因子提升为 approved factors。

每日信号生成：

```bash
make daily-signal SIGNAL_PROVIDER_CONFIG=configs/provider_current.yaml
```

目标组合生成：

```bash
make target-portfolio SIGNAL_CSV=reports/signals_20260420.csv
```

组合会保留 top factor driver 字段，用于后续解释：

- `top_factor_1`
- `top_factor_1_contribution`
- `top_factor_2`
- `top_factor_2_contribution`

## 6. 组合成熟度与风险门禁

当前组合 gate 包括：

- 单票权重上限
- 最小持仓数
- 信号覆盖率
- 最大换手
- 事件风险阻断
- 行业集中度
- 因子族数量
- 单一因子族贡献集中度

新增暴露归因入口：

```bash
make exposure-attribution EXPOSURE_INPUT=reports/target_portfolio_20260420.csv
```

输出：

- summary
- factor families
- industry
- style

这一步回答的问题不是“组合会不会涨”，而是“组合到底押了什么”。

## 7. 专家复核与纸面执行

专家复核 packet 汇总：

- 组合候选
- 单因子诊断
- 行业、流动性、事件风险
- 公告/异动上下文

gate 规则：

- `reject`: 阻断
- `caution`: 降仓或要求人工确认
- `approve/not_run`: 继续执行后续纸面流程

纸面执行链路：

- 订单生成: `scripts/generate_orders.py`
- 模拟成交: `paper_broker`
- 仓位更新: `state`
- 对账: `reconcile`
- 批量纸面回放: `scripts/run_paper_batch.py`

## 8. 当前系统定位

当前 factor-lab 更像一个“AI 辅助 A 股因子投研工作台”，而不是自动荐股或全自动实盘系统。

它的核心价值是：

- 让因子研究可复现
- 让 agent 自动试错但不越界
- 让组合进入执行前可解释、可审查、可阻断
- 让人工复核聚焦在证据链、风险和组合暴露上
