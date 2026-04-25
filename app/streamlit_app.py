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
    build_gate_review_items,
    build_portfolio_gate_explanation,
    build_pretrade_review,
    build_research_pipeline_status,
    classify_gate_decision,
    find_latest_run_dir,
    find_latest_target_portfolio,
    get_candidate_diagnostics,
    get_candidate_artifacts,
    load_execution_gate_card,
    load_autoresearch_queue,
    load_factor_family_map_safe,
    load_portfolio_gate_explanation,
    load_risk_config_dict,
    load_workbench_snapshot,
    parse_expert_review_result,
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

    render_execution_gate_card(load_execution_gate_card(ROOT))

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

    st.subheader("今日投研队列")
    st.caption("把 nightly 研究、专家复核、portfolio gate 和纸面订单串成一个可追踪状态流。")
    st.dataframe(build_research_pipeline_status(ROOT), use_container_width=True, hide_index=True)

    st.subheader("产物新鲜度")
    st.caption("用红黄绿灯判断当前工作台读到的是不是最近一轮研究产物。")
    freshness = pd.DataFrame(snapshot.freshness)
    if not freshness.empty:
        freshness["灯号"] = freshness["status"].map({"ready": "green", "stale": "yellow", "missing": "red"}).fillna("gray")
        st.dataframe(
            freshness.loc[:, ["灯号", "label", "status", "age_hours", "path"]],
            use_container_width=True,
            hide_index=True,
        )

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
    render_execution_gate_card(load_execution_gate_card(ROOT))

    expert = _latest_expert_review()
    st.subheader("专家复核结构化摘要")
    expert_cols = st.columns(3)
    expert_cols[0].metric("status", expert["status"])
    expert_cols[1].metric("decision", expert["decision"])
    expert_cols[2].metric("watchlist", len(expert["watchlist"]))
    if expert["summary"]:
        st.write(expert["summary"])
    if expert["risk_notes"]:
        st.caption(expert["risk_notes"])
    if expert["watchlist"]:
        st.dataframe(pd.DataFrame({"instrument": expert["watchlist"]}), use_container_width=True, hide_index=True)

    st.subheader("为什么被 caution / reject")
    st.dataframe(_gate_frame(gate.checks), use_container_width=True, hide_index=True)
    review_items = build_gate_review_items(gate.checks)
    if review_items.empty:
        st.success("当前 gate 没有失败项。")
    else:
        st.subheader("前置复核动作")
        st.caption("caution 项可以降仓或人工确认；reject 项默认阻断组合进入纸面执行。")
        st.dataframe(review_items, use_container_width=True, hide_index=True)

    st.subheader("交易前检查")
    st.caption("覆盖公告/异动、涨跌停、停牌、流动性和硬性 risk flags。")
    pretrade = build_pretrade_review(portfolio)
    st.dataframe(pretrade, use_container_width=True, hide_index=True)

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

    st.subheader("候选对比")
    chart_frame = filtered.dropna(subset=["primary_metric", "complexity_score"])
    if chart_frame.empty:
        st.info("当前筛选结果缺少可绘制的 primary_metric / complexity_score。")
    else:
        st.scatter_chart(
            chart_frame,
            x="complexity_score",
            y="primary_metric",
            color="status",
            size=80,
        )

    if not filtered.empty:
        options = filtered["candidate_name"].fillna("").astype(str).tolist()
        selected = st.selectbox("候选详情", options=options)
        selected_row = filtered.loc[filtered["candidate_name"].astype(str) == selected].head(1)
        if not selected_row.empty:
            selected_artifact = selected_row.iloc[0].get("artifact_dir", "")
            artifacts = get_candidate_artifacts(ROOT, selected_artifact)
            diagnostics = get_candidate_diagnostics(ROOT, selected, selected_artifact)
            st.caption(f"artifact: {artifacts['artifact_dir']}")
            if not diagnostics["eval"].empty:
                st.subheader("IC / 收益摘要")
                st.dataframe(diagnostics["eval"], use_container_width=True, hide_index=True)
            if not diagnostics["yearly"].empty:
                st.subheader("年度稳定性")
                yearly = diagnostics["yearly"]
                st.bar_chart(yearly.set_index("segment")["neutral_rank_ic_mean"])
                st.dataframe(yearly, use_container_width=True, hide_index=True)
            if not diagnostics["redundancy"].empty:
                st.subheader("重复因子簇")
                st.dataframe(diagnostics["redundancy"], use_container_width=True, hide_index=True)
            if artifacts["summary"]:
                st.code(artifacts["summary"], language="yaml")
            if artifacts["candidate"]:
                with st.expander("candidate.yaml"):
                    st.code(artifacts["candidate"], language="yaml")


def _gate_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    result = frame.copy()
    result["decision_level"] = result.apply(lambda row: _decision_level(row["check"], row["status"]), axis=1)
    return result


def render_execution_gate_card(card: dict) -> None:
    st.subheader("纸面执行总判定")
    cols = st.columns(3)
    cols[0].metric("decision", str(card.get("decision", "")).upper())
    cols[1].metric("action", str(card.get("action", "")))
    cols[2].metric("reason_count", len(card.get("reasons", [])))
    headline = str(card.get("headline", ""))
    if card.get("decision") == "reject":
        st.error(headline)
    elif card.get("decision") == "caution":
        st.warning(headline)
    else:
        st.success(headline)
    reasons = card.get("reasons", [])
    if reasons:
        st.dataframe(pd.DataFrame({"reason": reasons}), use_container_width=True, hide_index=True)


def _decision_level(check: str, status: str) -> str:
    if status == "pass":
        return "pass"
    synthetic = pd.DataFrame([{"check": check, "status": status}])
    return classify_gate_decision(synthetic)


def _latest_expert_review() -> dict:
    latest_run = find_latest_run_dir(ROOT)
    if latest_run is None:
        return parse_expert_review_result("")
    return parse_expert_review_result(latest_run / "expert_review_result.md")


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
