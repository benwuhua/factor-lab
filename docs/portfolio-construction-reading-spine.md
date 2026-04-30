# Portfolio Construction Reading Spine

本文档记录 factor-lab 组合层的参考框架。它的目标不是把研究候选直接变成买卖建议，而是把 alpha、风险、成本、约束和人工复核串成一个可审计的组合构建流程。

## 1. 项目内的组合层定义

factor-lab 把组合分成三层：

1. `research_portfolio`: 研究候选组合。它回答“当前因子系统最想研究哪些股票”。
2. `execution_portfolio`: 执行候选组合。它回答“经过专家复核、风险门禁和交易约束后，还允许进入纸面执行的权重是什么”。
3. `target_portfolio`: 兼容旧脚本的执行组合别名。后续新代码应优先读取 `execution_portfolio`。

组合层的核心目标函数可以抽象为：

```text
maximize:
  expected_alpha
  - risk_penalty
  - turnover_penalty
  - transaction_cost_penalty
  - constraint_penalty
```

其中 `expected_alpha` 来自因子和研究信号；后四项来自组合工程、交易现实和审查约束。

## 2. 参考“圣经”

### Active Portfolio Management

Richard Grinold 和 Ronald Kahn 的 *Active Portfolio Management* 是组合层的第一本主参考。

它对 factor-lab 最有用的部分：

- alpha 不等于持仓，alpha 必须经过风险预算和组合构建。
- Information Ratio、active risk 和 breadth 决定研究能力能转化成多少组合收益。
- 每个信号都要问：预测强度、覆盖宽度、换手成本和风险暴露是否匹配。

项目映射：

```text
factor score -> expected_alpha
IC / ICIR -> alpha confidence
risk gate -> active risk budget
execution_portfolio -> constrained active portfolio
```

### Quantitative Equity Portfolio Management

Qian、Hua、Sorensen 的 *Quantitative Equity Portfolio Management* 是工程落地层的主参考。

它对 factor-lab 最有用的部分：

- alpha 模型、风险模型、交易成本模型和组合优化器必须分开。
- 单因子好不代表组合好，组合好要看风险、换手、行业、市值和交易约束后的表现。
- 组合系统需要归因和反馈闭环，不能只看当日 Top 股票。

项目映射：

```text
factor registry / combo spec -> alpha model
risk.yaml / exposure attribution -> risk model
orders / paper broker -> trading cost and execution layer
run bundle / replay -> feedback loop
```

### Modern Portfolio Theory and Mean-Variance

Markowitz 的均值-方差框架是底层数学基础，但不应被当成直接可用的实盘系统。

它对 factor-lab 最有用的部分：

- 权重不是排序结果，而是风险收益权衡的解。
- 协方差、集中度和约束会显著改变“最优”持仓。
- 估计误差很大时，朴素优化容易产生极端权重。

项目落地原则：

- MVP 先用 Top N 等权、单票上限和行业/因子族门禁。
- 等数据稳定后再接入协方差矩阵、风险模型和优化器。
- 每次优化都必须输出“为什么不是简单 Top N”的解释。

### Black-Litterman

Black-Litterman 更适合把研究观点、主题观点和基准权重融合。

它对 factor-lab 最有用的部分：

- 不直接用模型分数替代市场均衡，而是把研究观点作为可置信的偏离。
- 适合处理主题行情，例如半导体、AI 算力、红利防御等风格观点。
- 观点需要置信度，置信度会影响权重偏离。

项目映射：

```text
theme score / expert view -> views
benchmark universe weights -> prior
review confidence -> view confidence
execution_portfolio -> posterior constrained weights
```

### Barra / Axioma 风险模型体系

Barra 和 Axioma 类风险模型不是一本书，但它们是实务组合层的行业参考。

它对 factor-lab 最有用的部分：

- 组合收益必须拆成行业、风格、特质和残差。
- 多因子组合最容易无意中变成小市值、单行业或低估值押注。
- 风险模型应当服务于“看清押注”，不是只服务于优化。

项目映射：

```text
industry exposure -> industry risk
factor family exposure -> style / alpha crowding
logic bucket exposure -> signal redundancy
event risk -> idiosyncratic risk overlay
```

## 3. factor-lab 当前采用的组合准则

当前阶段采用保守组合工程：

- 研究组合可以激进，执行组合必须审慎。
- 因子分数只负责排序，不直接负责仓位。
- `caution` 允许降仓或人工确认，`reject` 必须阻断。
- 组合通过前必须看行业集中、因子族集中、事件风险、流动性和涨跌停约束。
- 任何 Top 股票名单都只能作为研究候选，不能单独作为交易结论。

## 4. 后续演进路径

短期：

- 用 `research_portfolio` 和 `execution_portfolio` 固定语义边界。
- 在 UI 中并排展示“研究理由”和“执行阻断/降仓理由”。
- 对每日组合做归因复盘：今日涨跌来自市场、行业、风格还是个股。

中期：

- 建立简化风险模型：行业、规模代理、波动率、流动性、因子族暴露。
- 建立交易成本模型：换手、成交额占比、涨跌停、停牌和滑点。
- 组合构建从 Top N 等权升级到约束打分加权。

长期：

- 引入协方差风险模型和优化器。
- 引入 Black-Litterman 风格的主题观点融合。
- 建立组合绩效归因与模型反馈，让因子、组合和执行形成闭环。
