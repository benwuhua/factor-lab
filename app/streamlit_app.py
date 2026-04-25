from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import streamlit as st

from qlib_factor_lab.workbench import (
    build_portfolio_gate_explanation,
    classify_gate_decision,
    find_latest_target_portfolio,
    load_autoresearch_queue,
    load_factor_family_map_safe,
    load_portfolio_gate_explanation,
    load_risk_config_dict,
    load_workbench_snapshot,
    summarize_autoresearch_queue,
)


st.set_page_config(page_title="Factor Lab Workbench", layout="wide")


def main() -> None:
    _style()
    st.sidebar.title("Factor Lab")
    st.sidebar.caption("AI 辅助 A 股因子投研工作台")
    page = st.sidebar.radio(
        "导航",
        ["总览仪表盘", "Portfolio Gate 解释", "Autoresearch 队列"],
        label_visibility="collapsed",
    )
    st.sidebar.divider()
    st.sidebar.caption("数据边界")
    st.sidebar.write("CSI300 / CSI500")
    st.sidebar.caption("当前模式")
    st.sidebar.write("只读研究工作台")

    if page == "总览仪表盘":
        render_dashboard()
    elif page == "Portfolio Gate 解释":
        render_portfolio_gate()
    else:
        render_autoresearch_queue()


def render_dashboard() -> None:
    snapshot = load_workbench_snapshot(ROOT)
    queue = load_autoresearch_queue(ROOT)
    gate = load_portfolio_gate_explanation(ROOT)
    target_name = snapshot.latest_target_portfolio.name if snapshot.latest_target_portfolio else "missing"

    st.title("投研业务流总览")
    st.caption("从数据治理、因子挖掘、组合门禁到专家复核和纸面执行的本地研究工作台。")

    cols = st.columns(5)
    cols[0].metric("Approved Factors", snapshot.approved_factor_count)
    cols[1].metric("Autoresearch Review", snapshot.autoresearch_status_counts.get("review", 0))
    cols[2].metric("Portfolio Gate", gate.decision.upper())
    cols[3].metric("Latest Target", target_name)
    cols[4].metric("Paper Bundle", "Ready" if snapshot.latest_run_dir else "No run")

    st.subheader("流水线操作台")
    st.caption("第一版为只读操作台：展示建议命令和产物状态，不直接从 UI 执行本地任务。")
    command_rows = pd.DataFrame(
        [
            {"步骤": "01 DATA", "动作": "检查数据与交易日", "命令": "make check-env", "状态": "ready"},
            {"步骤": "02 RESEARCH", "动作": "启动候选因子挖掘", "命令": "make autoresearch-codex-loop", "状态": "review"},
            {"步骤": "03 GOVERN", "动作": "生成 approved 因子", "命令": "make select-factors", "状态": "governed"},
            {"步骤": "04 SIGNAL", "动作": "构建当日信号", "命令": "make daily-signal", "状态": "built"},
            {"步骤": "05 PORTFOLIO", "动作": "组合与暴露门禁", "命令": "make target-portfolio", "状态": gate.decision},
            {"步骤": "06 PAPER", "动作": "纸面订单与对账", "命令": "make paper-orders", "状态": "ready"},
        ]
    )
    st.dataframe(command_rows, use_container_width=True, hide_index=True)

    left, right = st.columns([1.15, 0.85])
    with left:
        st.subheader("Portfolio Gate 快照")
        st.dataframe(_gate_frame(gate.checks), use_container_width=True, hide_index=True)
    with right:
        st.subheader("Autoresearch 状态")
        summary = summarize_autoresearch_queue(queue)
        st.bar_chart(pd.Series(summary, name="count"))
        st.caption("来源: reports/autoresearch/expression_results.tsv")


def render_portfolio_gate() -> None:
    st.title("Portfolio Gate 解释")
    latest = find_latest_target_portfolio(ROOT)
    if latest is None:
        st.warning("还没有找到 target_portfolio CSV。先运行 make target-portfolio。")
        return

    portfolio = pd.read_csv(latest)
    risk_config = load_risk_config_dict(ROOT)
    gate = build_portfolio_gate_explanation(
        portfolio,
        risk_config=risk_config,
        factor_family_map=load_factor_family_map_safe(ROOT),
    )

    status = gate.decision.upper()
    st.metric("Gate Decision", status)
    st.caption(f"目标组合: {latest}")
    st.subheader("为什么被 caution / reject")
    st.dataframe(_gate_frame(gate.checks), use_container_width=True, hide_index=True)

    chart_cols = st.columns(2)
    with chart_cols[0]:
        st.subheader("行业权重")
        if gate.industry.empty:
            st.info("组合里没有行业字段。")
        else:
            st.bar_chart(gate.industry.set_index("industry")["weight"])
    with chart_cols[1]:
        st.subheader("因子族贡献")
        if gate.family.empty:
            st.info("组合里没有 top factor driver。")
        else:
            st.bar_chart(gate.family.set_index("family")["abs_weighted_contribution"])

    st.subheader("组合明细")
    keep = [
        column
        for column in [
            "date",
            "instrument",
            "rank",
            "target_weight",
            "ensemble_score",
            "industry",
            "top_factor_1",
            "top_factor_1_contribution",
            "top_factor_2",
            "top_factor_2_contribution",
            "risk_flags",
            "event_risk_summary",
        ]
        if column in portfolio.columns
    ]
    st.dataframe(portfolio.loc[:, keep] if keep else portfolio, use_container_width=True, hide_index=True)


def render_autoresearch_queue() -> None:
    st.title("Autoresearch Nightly 队列")
    queue = load_autoresearch_queue(ROOT)
    if queue.empty:
        st.warning("还没有 expression_results.tsv。先运行 make autoresearch-expression 或 autoresearch-codex-loop。")
        return

    summary = summarize_autoresearch_queue(queue)
    cols = st.columns(3)
    cols[0].metric("Review", summary["review"])
    cols[1].metric("Discard", summary["discard_candidate"])
    cols[2].metric("Crash", summary["crash"])

    status = st.multiselect(
        "状态过滤",
        options=sorted(queue["status"].dropna().astype(str).unique()),
        default=["review"] if "review" in set(queue["status"].astype(str)) else [],
    )
    filtered = queue[queue["status"].astype(str).isin(status)] if status else queue
    display_cols = [
        "timestamp",
        "candidate_name",
        "status",
        "primary_metric",
        "neutral_rank_ic_mean_h20",
        "complexity_score",
        "decision_reason",
        "artifact_dir",
    ]
    st.dataframe(filtered.loc[:, [column for column in display_cols if column in filtered.columns]], use_container_width=True)

    top = filtered.head(1)
    if not top.empty:
        artifact = ROOT / str(top.iloc[0].get("artifact_dir", ""))
        summary_path = artifact / "summary.txt"
        candidate_path = artifact / "candidate.yaml"
        st.subheader("最近候选详情")
        if summary_path.exists():
            st.code(summary_path.read_text(encoding="utf-8"), language="yaml")
        if candidate_path.exists():
            with st.expander("candidate.yaml"):
                st.code(candidate_path.read_text(encoding="utf-8"), language="yaml")


def _gate_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    result["decision_level"] = result.apply(lambda row: _decision_level(row["check"], row["status"]), axis=1)
    return result


def _decision_level(check: str, status: str) -> str:
    if status == "pass":
        return "pass"
    synthetic = pd.DataFrame([{"check": check, "status": status}])
    return classify_gate_decision(synthetic)


def _style() -> None:
    st.markdown(
        """
        <style>
          .block-container { padding-top: 2rem; padding-bottom: 3rem; }
          section[data-testid="stSidebar"] { background: #18211e; }
          section[data-testid="stSidebar"] * { color: #edf2ee; }
          div[data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #cfd5cf;
            border-radius: 8px;
            padding: 14px;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
