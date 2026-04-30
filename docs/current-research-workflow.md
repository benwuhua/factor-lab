# Factor Lab Current Research Workflow

本文梳理当前 factor-lab 从数据、因子、自动挖掘、组合、专家复核到纸面执行的业务流程。

项目的长期主指导文档是 [factor-lab-north-star-blueprint.md](factor-lab-north-star-blueprint.md)。当前 workflow 记录“现在怎么跑”，North-Star 蓝图记录“后续应该补齐到什么形态”。

## 1. 数据治理

当前研究数据库固定服务两个 A 股核心股票池：

- 沪深300: `configs/provider_csi300_current.yaml`
- 中证500: `configs/provider_current.yaml`

主要数据入口：

- 行情与 OHLCV: `scripts/build_akshare_qlib_data.py`
- 证券主数据与公告事件: `scripts/build_research_context_data.py`
- 数据域覆盖率与时点门禁: `scripts/check_data_governance.py`
- 数据质量检查: `scripts/check_data_quality.py`

业务目标是保证研究、回测、组合和风控都在同一套股票池与同一套时点数据上运行。

当前缺口：

- 情绪气氛、基本面、股东与资本结构仍不是可回测的完整时点数据域。
- 公告事件已经能服务风险门禁和专家复核，但还需要补齐 `available_at`、`usable_for_trade_date` 和覆盖率报告。
- 后续新增数据域必须先通过 coverage、freshness、point-in-time 字段完整性和样本量门禁，再允许进入主组合。

运行入口：

```bash
make data-governance RUN_DATE=20260420
```

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

当前实现仍以 expression loop 为主；North-Star 目标是按 lane 并行运行：

- `expression_price_volume`
- `pattern_event`
- `emotion_atmosphere`
- `liquidity_microstructure`
- `risk_structure`
- `shareholder_capital`
- `fundamental_quality`
- `regime`

其中 `emotion_atmosphere`、`shareholder_capital`、`fundamental_quality` 在数据域未达标前只能 shadow research，不能直接进入主组合。

固定边界：

- provider: 沪深300 / 中证500
- horizons: 5 和 20
- purification: 默认 `mad -> zscore`
- neutralization: raw 和 size_proxy neutralized
- ledger: `reports/autoresearch/expression_results.tsv`

运行入口：

```bash
make autoresearch-expression
make autoresearch-multilane
make autoresearch-codex-loop AUTORESEARCH_CODEX_UNTIL=08:30 AUTORESEARCH_CODEX_ITERATIONS=30
```

每轮实验都会生成 summary block、raw/neutralized eval、candidate copy 和 ledger row。

当前 `autoresearch-multilane` 已提供调度层：`expression_price_volume` 会调用现有 expression oracle；`emotion_atmosphere`、`shareholder_capital`、`fundamental_quality` 在数据域未达标前记录为 shadow，不进入主组合。

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

研究组合与执行组合生成：

```bash
make target-portfolio SIGNAL_CSV=reports/signals_20260420.csv
```

正式日流水线会同时产出三类组合文件：

- `research_portfolio.csv`: 因子和信号层给出的研究候选，回答“值得研究谁”。
- `execution_portfolio.csv`: 专家复核、风险门禁和交易约束后的执行候选，回答“允许纸面执行多少权重”。
- `target_portfolio.csv`: 兼容旧脚本的执行组合别名，新代码应优先读取 `execution_portfolio.csv`。

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

正式的当日涨跌归因入口：

```bash
make portfolio-intraday-performance RUN_DATE=20260430
```

这一步会读取 `runs/<date>/execution_portfolio.csv`，抓取或读取报价快照，写出：

- `runs/<date>/portfolio_intraday_performance.csv`
- `runs/<date>/portfolio_intraday_performance.md`
- `reports/portfolio_intraday_performance_<date>.csv`
- `reports/portfolio_intraday_performance_<date>.md`

输出：

- summary
- factor families
- industry
- style

这一步回答的问题不是“组合会不会涨”，而是“组合到底押了什么”。

组合层参考框架见 [portfolio-construction-reading-spine.md](portfolio-construction-reading-spine.md)。当前阶段遵循的原则是：因子分数只生成研究候选，执行权重必须经过风险、成本、约束和复核。

## 7. 专家复核与纸面执行

专家复核 packet 汇总：

- 组合候选
- 单因子诊断
- 行业、流动性、事件风险
- 公告/异动上下文

股票卡片入口：

```bash
make stock-cards TARGET_PORTFOLIO=reports/target_portfolio_20260420.csv RUN_DATE=20260420
```

股票卡片把组合候选、因子 driver、事件证据、交易状态、门禁原因和审计字段写成 `reports/stock_cards_YYYYMMDD.jsonl`，用于 UI 和专家复核继续消费。

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
