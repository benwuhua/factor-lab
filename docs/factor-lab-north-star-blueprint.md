# Factor Lab North-Star Blueprint

本文是 factor-lab 的终局指导蓝图。它把数据治理、多类型因子挖掘、股票卡片、组合门禁、专家复核和纸面执行放进同一个投研闭环里，避免项目继续只沿着单一量价表达式扩展。

## 1. 定位

factor-lab 不是荐股产品，也不是自动实盘系统。它应该是一个受控的 A 股 AI 投研实验室：

- 用固定数据边界做可复现实验。
- 让 agent 在明确搜索空间里挖掘不同类型因子。
- 用单因子诊断和 family-first 组合避免同质化。
- 用股票卡片和证据库把量化结果变成人能审的投研对象。
- 通过组合门禁、专家复核和纸面执行防止研究结果直接越界。

## 2. 数据治理目标

当前数据层已经有沪深300和中证500日线 provider、证券主数据、公告事件和执行日历，但这还不够支撑多类型投研。North-Star 数据层要扩展为七类可追溯数据域。

| 数据域 | 当前状态 | 目标用途 | 更新频率 | 必要字段 |
|---|---|---|---|---|
| 行情 OHLCV | 已有 Qlib provider | 量价、波动、换手、执行日历 | 日频 | trade_date/as_of_date/open/high/low/close/volume/amount/turnover/adj/source_timestamp |
| 证券主数据 | 初版 CSV | 行业、上市状态、ST、板块、指数池归属 | 日频 | instrument/name/industry/board/list_date/st_status/research_universes/valid_from/valid_to |
| 公告事件 | 初版 CSV | 事件风险、专家复核、股票卡片证据 | 日频 | event_type/event_date/announce_date/available_at/severity/source_url/active_until/summary |
| 情绪气氛 | 缺失 | 气氛因子、过热/恐慌/拥挤判断 | 日频 | trade_date/as_of_date/available_at/limit_count/breadth/turnover_heat/amount_concentration/high_low_stats |
| 流动性与交易约束 | 部分已有 | 滑点代理、容量、买卖限制 | 日频 | trade_date/as_of_date/available_at/amount_20d/turnover_20d/limit_up/limit_down/suspended/buy_blocked |
| 基本面 | 缺失 | 估值、质量、成长、盈利修正 | 财报/周频 | announce_date/available_at/report_period/roe/growth/valuation/cashflow/factor_version |
| 股东与资本结构 | 缺失 | 股东因子、减持、回购、质押、解禁 | 周频/事件 | announce_date/event_date/effective_date/available_at/active_until/holder_count/buyback/pledge/unlock/reduction/increase |

关键原则：

- 所有数据域都必须声明 `as_of_date` 或 `available_at`，并能推导 `usable_for_trade_date`。
- 非行情数据必须额外带 `announce_date`、`effective_date`、`event_date` 或 `valid_from/valid_to` 中适合本域的一组字段。
- 盘后公告、盘后情绪统计、交易所盘后数据，默认只能从下一个交易日开始进入回测和组合。
- 任何不能保证时点正确的数据只能进入证据库或人工复核，不能直接进入回测。
- 数据覆盖率要成为 UI 的一等指标，而不是隐藏在日志里。
- 沪深300和中证500仍是默认研究边界，扩池必须单独审批。

### 2.1 数据可用性门禁

每个数据域进入因子、回测或组合前，必须先通过数据可用性门禁：

| 门禁 | 要求 |
|---|---|
| coverage | 沪深300和中证500覆盖率达到 lane 配置阈值 |
| freshness | 数据刷新时间不晚于配置允许的最大延迟 |
| point-in-time | `available_at`、`announce_date`、`effective_date` 等字段完整率达到阈值 |
| lag rule | 明确同日、次日或 T+N 可用规则 |
| sample size | 目标 lane 的事件数或截面样本数达到最低要求 |
| audit trail | 保留 source、source_timestamp、run_id 和数据版本 |

未通过门禁的数据域只能让对应 lane 处于 `shadow` 或 `disabled` 状态，不能进入主组合。

## 3. JoinQuant 分类到 Factor Lab Lane 的映射

Factor Lab 的 lane 不是照搬 JoinQuant 因子分类，而是把 JoinQuant 风格分类翻译成适合本项目研究闭环的可执行车道。

| JoinQuant 风格分类 | Factor Lab lane | 说明 |
|---|---|---|
| 动量、反转、技术、波动率 | `expression_price_volume` / `risk_structure` | 普通截面表达式、波动和风险结构 |
| 换手率、成交量、成交额 | `liquidity_microstructure` / `emotion_atmosphere` | 流动性是交易条件，气氛是市场状态 |
| 情绪类、分析师情绪 | `emotion_atmosphere` | 涨跌停、宽度、拥挤、热度、恐慌修复 |
| 估值、成长、质量 | `fundamental_quality` | 需要财务时点数据后再开放 |
| 股东因子 | `shareholder_capital` | 股东户数、回购、增减持、解禁、质押 |
| 风格/风险因子 | `risk_structure` / `regime` | 风险暴露和因子启停 |

第一阶段的 `emotion_atmosphere` 只做市场气氛，不做分析师一致预期或研报情绪。分析师预期需要单独的数据域 `analyst_expectation`，具备披露时间、机构来源、预测版本和可用时间后再接入。

## 4. 多 Lane Autoresearch

Nightly autoresearch 不应该只跑 expression。North-Star 目标是多 lane 并行，每个 lane 有独立搜索空间、候选文件、oracle、ledger 和复核规则。

| lane | 优先级 | cadence | 主要问题 | 主指标 |
|---|---:|---|---|---|
| `expression_price_volume` | P0 | daily | 普通量价表达式是否有稳定截面 alpha | neutral Rank IC / ICIR / long-short |
| `pattern_event` | P0 | daily | 王吉类盘形、突破、缩量回踩是否有事件收益 | event return / payoff / MFE-MAE |
| `emotion_atmosphere` | P0 | daily | 市场气氛、拥挤、热度、恐慌修复是否可解释收益 | event return / breadth conditional IC / turnover guard |
| `liquidity_microstructure` | P1 | daily | 流动性变化是否带来可交易 alpha 或风险过滤 | H5/H20 IC / slippage proxy |
| `risk_structure` | P1 | daily | 回撤质量、下行波动、跳空风险能否改善组合 | downside capture / tail filter |
| `shareholder_capital` | P2 | weekly/event | 回购、增减持、解禁、质押、股东变化是否有信号 | event return / risk block |
| `fundamental_quality` | P2 | weekly | 估值、质量、成长是否稳定补充量价 | H20/H60 IC / yearly stability |
| `regime` | P1 | daily | 什么时候启停某类因子 | drawdown reduction / activation accuracy |

每个 lane 的 agent 约束：

- 只能修改本 lane 的 candidate/spec。
- 不能改 provider、评估器、ledger schema。
- 必须写入 lane 独立 ledger。
- 必须跑重复度检查，防止同一思想换名重复。
- 必须输出能进入股票卡片的解释字段。

每个 lane 的启用状态由 `lane_activation` 决定：

| 状态 | 含义 |
|---|---|
| `active` | 数据覆盖、freshness、PIT 字段、样本量全部达标，可进入 approved/promotion 流程 |
| `shadow` | 可以研究和记录 ledger，但不能进入主组合 |
| `disabled` | 数据或评估契约不满足，只能保留配置，不运行 nightly |

尤其是 `emotion_atmosphere`、`shareholder_capital`、`fundamental_quality` 这些依赖新增数据域的 lane，必须先通过数据可用性门禁，不能因为分类已经写进蓝图就直接进入组合。

## 5. 气氛类因子

`emotion_atmosphere` 是 A 股必须有的 lane。它不是普通成交量因子，而是刻画市场参与者状态。

建议第一批 family：

- `turnover_heat`: 换手率短期放大、历史分位、连续高换手。
- `limit_pressure`: 涨跌停、炸板、连板、跌停压力。
- `breadth_mood`: 股票池上涨家数、创新高/新低、强势股扩散。
- `crowding`: 成交额集中度、行业拥挤、短线资金过热。
- `volatility_mood`: 大振幅、长上下影、情绪释放。
- `sentiment_reversal`: 过热降温、恐慌修复。

气氛类的 IC 只能作为辅助指标。更重要的是：

- 分气氛状态的条件收益。
- 高分样本 trade_count 是否足够。
- 高热度后是否容易回撤。
- 是否只是流动性暴露或小盘暴露。
- 是否能解释股票卡片里的入选原因。

## 6. 股东与资本结构因子

`shareholder_capital` 是慢变量和事件变量混合 lane。它更像风险/催化证据，而不是每日高频 alpha。

建议第一批 family：

- `holder_count_change`: 股东户数变化、户均持股变化。
- `buyback`: 回购计划、回购进展、回购完成。
- `pledge_risk`: 股权质押比例、平仓风险、补充质押。
- `unlock_overhang`: 限售解禁规模、解禁占流通股比例。
- `reduction_increase`: 股东/董监高减持、增持、承诺变更。
- `capital_structure_change`: 定增、可转债、股本变化。

评价重点：

- 事件收益和事件后最大回撤，而不是普通日频 IC。
- 事件覆盖率和公告时点正确性。
- 风险阻断精度，例如高质押、临近解禁、密集减持是否帮助 portfolio gate 避险。
- 数据缺口必须进入股票卡片和专家复核，不能静默忽略。

## 7. 股票卡片

股票卡片是因子层和人工投研之间的核心对象。它不是行情卡，也不是荐股卡，而是一个可审查证据包。

```text
Stock Research Card =
security master
+ current signal
+ factor family exposure
+ emotion/liquidity/risk state
+ event evidence
+ portfolio role
+ review questions
```

卡片字段分层：

| 模块 | 字段 |
|---|---|
| 基础身份 | instrument/name/research_universes/industry/board/ST/listing_status |
| 当前信号 | ensemble_score/rule_score/model_score/top_factor_1/top_factor_2/family_contribution |
| 因子画像 | expression/pattern/emotion/liquidity/risk/fundamental/shareholder score |
| 交易状态 | target_weight/amount_20d/turnover_20d/limit_up/limit_down/suspended/buy_blocked |
| 证据链 | event_count/max_event_severity/event_types/source_urls/recent_announcements |
| 组合角色 | new/add/reduce/hold/industry contribution/family contribution |
| 复核问题 | why_selected/key_risk/manual_chart_needed/manual_announcement_needed/gate_reason |
| 审计闭环 | run_id/as_of_date/card_version/factor_version/evidence_id/review_decision/reviewer/reviewed_at/override_reason |

使用方式：

1. 每个 `target_portfolio` 生成一批 `stock_cards_YYYYMMDD.jsonl`。
2. UI 增加“个股卡片”页，可按组合、行业、factor family、risk flag 过滤。
3. 专家复核 packet 引用 top N stock cards，而不是只看组合 CSV。
4. `caution/reject` 必须能回溯到股票卡片里的字段或证据。

## 8. Family-First 组合

approved factors 不应该直接平铺相加。组合层先生成 family score，再做最终 ensemble。

推荐流程：

1. 单因子诊断通过后进入 approved pool。
2. 相似候选按 family 聚类，只保留代表因子进入主分。
3. reserve 因子只 shadow scoring，不影响主组合。
4. 每个 family 有权重上限。
5. `regime` lane 只控制 family 权重，不直接选股。
6. 组合门禁检查 family concentration、行业集中、流动性和事件风险。

初始 family 权重建议：

| family group | target weight |
|---|---:|
| expression_price_volume | 0.22 |
| pattern_event | 0.16 |
| emotion_atmosphere | 0.18 |
| liquidity_microstructure | 0.14 |
| risk_structure | 0.14 |
| fundamental_quality | 0.08 |
| shareholder_capital | 0.04 |
| regime overlay | 0.04 |

这些权重不是优化结果，只是防止早期组合被某一个量价 cluster 吃掉。

实际组合只能使用 `active` lane。上表是数据治理达标后的目标权重；`shadow` lane 只能生成旁路分数、股票卡片证据和复核提示，不能贡献主组合权重。

## 9. UI 目标

工作台 UI 最终应围绕五个问题组织：

1. 数据够不够：数据治理页显示各数据域覆盖率、freshness、时点字段完整性。
2. 今天挖了什么：自动挖掘页按 lane 展示 nightly 队列和重复簇。
3. 哪些因子能用：因子研究页展示单因子诊断、family、promotion/shadow 状态。
4. 为什么选这些股票：股票卡片页解释每个标的的因子、证据、风险和组合角色。
5. 能不能执行：组合门禁、专家复核和纸面执行页阻断不成熟输出。

## 10. 实施路线

### Stage 1: Blueprint Alignment

- 更新 `configs/autoresearch/lane_space.yaml`，加入 `emotion_atmosphere` 和 `shareholder_capital`。
- 在 README 和 workflow 文档里把 North-Star 蓝图设为主指导文档。
- UI 首页增加“当前实现 vs North-Star 缺口”提示。

### Stage 2: Data Governance Upgrade

- 新增 `configs/data_governance.yaml`。
- 新增数据覆盖率报告：行情、主数据、事件、情绪、流动性、基本面、股东。
- 将 `research-context` 扩展成可重复数据任务，而不是只生成两个 CSV。
- 输出 `lane_activation` 状态，未达标 lane 只能 `shadow` 或 `disabled`。

### Stage 3: Multilane Autoresearch MVP

- 实现 `run_multilane_autoresearch.py`。
- 首批跑三条 lane：`expression_price_volume`、`pattern_event`、`emotion_atmosphere`。
- 每条 lane 写独立 ledger，再汇总到 `reports/autoresearch/multilane_summary.md`。
- `emotion_atmosphere` 首次上线必须先跑 shadow，覆盖率、freshness、PIT、样本量全部达标后再允许进入 promotion。

### Stage 4: Stock Cards

- 实现 `build_stock_cards.py`。
- 输出 `reports/stock_cards_YYYYMMDD.jsonl` 和 `runs/YYYYMMDD/stock_cards.md`。
- UI 增加“个股卡片”页。
- 专家复核 packet 接入股票卡片。

### Stage 5: Family-First Portfolio

- 日信号从 factor-first 改为 family-first。
- portfolio gate 使用 family caps 和股票卡片证据。
- regime lane 只作为 family 权重 overlay。

## 11. 非目标

- 不把 LLM 评价当成收益证明。
- 不在数据不具备时点字段前上线基本面回测。
- 不让 pattern/event 因子只凭 IC 通过。
- 不让气氛因子绕过流动性和拥挤度检查。
- 不把 factor-lab 变成自动实盘下单系统。
