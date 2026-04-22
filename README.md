# Qlib Factor Lab

这是一个轻量的 Qlib 因子工厂脚手架，用来管理、评估和筛选公式型因子。

默认数据目录：

```text
data/qlib/cn_data
```

## 1. 创建环境

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -e .
```

## 2. 下载 Qlib CN 日频公开数据

```bash
python scripts/download_qlib_data.py
```

该脚本会按 Qlib 官方推荐的 `scripts/get_data.py qlib_data --target_dir ... --region cn` 路线下载数据。

## 3. 检查环境和数据

```bash
python scripts/check_env.py
```

## 4. 评估一个因子

```bash
python scripts/eval_factor.py --factor ret_20
```

报告会写到：

```text
reports/factor_ret_20.csv
```

## 5. 批量评估

```bash
python scripts/batch_eval_factors.py
```

报告会写到：

```text
reports/factor_batch.csv
```

也可以加上规模代理中性化和图表：

```bash
python scripts/eval_factor.py --factor ret_20 --neutralize-size-proxy --plot --plot-horizon 5
python scripts/batch_eval_factors.py --neutralize-size-proxy --plot-top
```

当前公开 Qlib CN 日频数据没有行业和市值字段，所以这里默认提供两层能力：

- `--neutralize-size-proxy`: 使用 `log(close * volume)` 作为规模/流动性代理做横截面中性化。
- `--industry-map path/to/industry.csv`: 如果你有自己的行业映射 CSV，可用 `instrument,industry` 两列接入行业中性化。

示例行业文件：

```csv
instrument,industry
SH600000,bank
SZ000001,bank
```

## 6. 候选因子挖掘

候选模板在：

```text
configs/factor_mining.yaml
```

只生成候选公式表：

```bash
python scripts/mine_factors.py --generate-only
```

批量筛选候选因子：

```bash
python scripts/mine_factors.py --horizon 5 --horizon 10 --horizon 20
```

冒烟测试可以少跑几个：

```bash
python scripts/mine_factors.py --limit 2 --horizon 5 --output reports/factor_mining_smoke.csv
```

输出：

```text
reports/factor_mining_candidates.csv
reports/factor_mining_results.csv
```

## 7. LightGBM 训练和回测

渲染 Qlib workflow 配置但不运行：

```bash
python scripts/run_lgb_workflow.py --dry-run
```

运行 Qlib Alpha158 + LightGBM + TopK 回测：

```bash
python scripts/run_lgb_workflow.py
```

脚本会根据本地 `configs/provider.yaml` 自动生成：

```text
configs/qlib_lgb_workflow.yaml
```

Qlib 的训练记录会写入本地：

```text
mlruns/
```

注意：这个项目目录不是 git 仓库时，Qlib recorder 会打印几段 `git diff` / `git status` 警告。它不影响训练和回测，只是 Qlib 想记录代码版本但找不到 git 元数据。

## 8. AkShare 当前 A 股数据

默认的 Qlib 官方公开包适合教学和流程验证，但数据只到 2020 年。要做当前研究，可以用 AkShare 构建一个独立的当前数据目录：

```bash
python scripts/build_akshare_qlib_data.py \
  --universe csi500 \
  --start 20150101 \
  --end 20260420 \
  --history-source sina \
  --qlib-dir data/qlib/cn_data_current \
  --source-dir data/akshare/source \
  --provider-config configs/provider_current.yaml
```

这个目录使用当前中证 500 成分股，日频前复权行情，默认字段包括：

```text
open, close, high, low, volume, amount, vwap, factor
```

当前数据配置：

```text
configs/provider_current.yaml
```

使用当前数据跑单因子：

```bash
python scripts/eval_factor.py \
  --provider-config configs/provider_current.yaml \
  --factor ret_20 \
  --output reports/factor_ret_20_current.csv
```

使用当前数据生成 LightGBM workflow：

```bash
python scripts/run_lgb_workflow.py \
  --provider-config configs/provider_current.yaml \
  --output configs/qlib_lgb_workflow_current.yaml \
  --dry-run
```

去掉 `--dry-run` 会直接训练并运行组合回测。当前 AkShare 股票包没有指数行情文件时，workflow 会自动把候选池股票日收益等权平均作为回测基准；如果后续补入 `SH000905` 指数行情，则优先使用中证 500 指数基准。

当前数据的建议训练切分：

```text
train: 2015-01-01 ~ 2021-12-31
valid: 2022-01-01 ~ 2023-12-31
test:  2024-01-01 ~ 2026-04-17
```

说明：

- AkShare 免费源适合研究原型和个人离线实验，正式生产研究最好接 Tushare、Wind、聚宽或券商数据。
- Sina 源大批量下载可能限流；脚本支持 `--delay`、`--retries` 和 `--limit`。
- `SH689009` 这类特殊股票如果 Sina 源缺数据，可以用腾讯源补单只股票。

## 因子库

因子定义在 `factors/registry.yaml` 中，每个因子包含：

- `name`: 因子名
- `expression`: Qlib 表达式
- `direction`: 预期方向，`1` 表示越大越好，`-1` 表示越小越好
- `category`: 因子类别
- `description`: 逻辑说明

先用简单、可解释的价量因子跑通，再逐步加入财务因子或自动生成因子。

## 因子挖掘候选池

候选池定义在：

```text
configs/factor_mining.yaml
```

当前候选池包含 36 个量价表达式，分为 6 类：

```text
candidate_momentum      动量：收益、跳过近端收益
candidate_reversal      反转：短期反转、日内反转
candidate_volatility    波动：收益波动、高低价区间波动
candidate_volume_price  量价：成交量动量、价量相关
candidate_liquidity     流动性：成交额、Amihud 式非流动性
candidate_divergence    背离：价格动量与成交量动量背离
```

生成候选目录：

```bash
python scripts/mine_factors.py \
  --config configs/factor_mining.yaml \
  --provider-config configs/provider_current.yaml \
  --candidates-output reports/factor_mining_candidates_current.csv \
  --generate-only
```

在当前中证 500 数据上做初筛：

```bash
python scripts/mine_factors.py \
  --config configs/factor_mining.yaml \
  --provider-config configs/provider_current.yaml \
  --candidates-output reports/factor_mining_candidates_current.csv \
  --output reports/factor_mining_current.csv \
  --horizon 5 \
  --horizon 20
```

筛选结果会按 `abs_rank_ic_mean` 排序。`rank_ic_mean` 为负时，说明该因子在这段样本里更像“反向使用”的候选，后续可以把方向翻转后再进模型或回测。

## 推荐使用顺序

1. 先用 `scripts/eval_factor.py` 看单因子的 IC、RankIC、换手和分组收益。
2. 再用 `scripts/mine_factors.py` 批量扩展价量候选因子。
3. 最后用 `scripts/run_lgb_workflow.py` 把多因子特征接入模型训练和组合回测。
