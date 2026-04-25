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
    st.sidebar.caption("AI 辅助 A 股因子投研工作台\n从候选因子到组合复核")
    page = st.sidebar.radio(
        "导航",
        ["01 总览仪表盘", "02 数据治理", "03 因子研究", "04 自动挖掘", "05 组合门禁", "06 专家复核", "07 纸面执行"],
        label_visibility="collapsed",
    )
    st.sidebar.divider()
    st.sidebar.caption("数据边界")
    st.sidebar.write("CSI300 / CSI500")
    st.sidebar.caption("当前模式")
    st.sidebar.write("只读研究工作台")

    if page == "04 自动挖掘":
        render_autoresearch_queue()
    elif page in {"05 组合门禁", "06 专家复核"}:
        render_portfolio_gate()
    else:
        render_dashboard()


def render_dashboard() -> None:
    snapshot = load_workbench_snapshot(ROOT)
    queue = load_autoresearch_queue(ROOT)
    gate = load_portfolio_gate_explanation(ROOT)
    execution_card = load_execution_gate_card(ROOT)
    pipeline = _pipeline_rows(gate.decision)

    st.markdown(
        """
        <div class="topbar">
          <div>
            <h1>投研业务流总览</h1>
            <div class="subtitle">固定沪深300和中证500数据边界，使用受控 autoresearch 扩展候选因子，通过净化、评价、暴露归因、风险门禁和专家复核后进入纸面执行。</div>
          </div>
          <div class="controls"><span>沪深300</span><span>中证500</span><span>5D / 20D</span><span>Export</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _metric_strip(
        [
            ("Approved Factors", snapshot.approved_factor_count),
            ("Autoresearch Status", "Review"),
            ("Purification", "MAD + Z"),
            ("Portfolio Gate", gate.decision.upper()),
            ("Paper Bundle", "Ready" if snapshot.latest_run_dir else "No run"),
        ]
    )

    main_col, rail_col = st.columns([3.1, 1.05], gap="large")
    with main_col:
        _execution_gate_panel(execution_card)
        _pipeline_ops_cards(pipeline)
        _flow_map()

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
    with rail_col:
        _right_rail(gate)


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
    _execution_gate_panel(card)


def _metric_strip(items: list[tuple[str, object]]) -> None:
    html = ['<section class="metrics">']
    for label, value in items:
        html.append(f'<div class="metric"><label>{_html(label)}</label><strong>{_html(value)}</strong></div>')
    html.append("</section>")
    st.markdown("".join(html), unsafe_allow_html=True)


def _pipeline_rows(gate_decision: str) -> list[dict[str, str]]:
    return [
        {"step": "01 DATA", "title": "检查数据与交易日", "command": "make check-env", "status": "ready", "action": "Run"},
        {"step": "02 RESEARCH", "title": "启动候选因子挖掘", "command": "make autoresearch-codex-loop", "status": "review", "action": "Queue"},
        {"step": "03 GOVERN", "title": "生成 approved 因子", "command": "make select-factors", "status": "governed", "action": "Open"},
        {"step": "04 SIGNAL", "title": "构建当日信号", "command": "make daily-signal", "status": "built", "action": "Run"},
        {"step": "05 PORTFOLIO", "title": "组合与暴露门禁", "command": "make target-portfolio", "status": gate_decision, "action": "Review"},
        {"step": "06 PAPER", "title": "纸面订单与对账", "command": "make paper-orders", "status": "ready", "action": "Stage"},
    ]


def _pipeline_ops_cards(rows: list[dict[str, str]]) -> None:
    cards = []
    for row in rows:
        cards.append(
            '<div class="op-card">'
            f'<span class="op-index">{_html(row["step"])}</span>'
            f'<b>{_html(row["title"])}</b>'
            f'<code>{_html(row["command"])}</code>'
            f'<div class="op-footer"><span class="op-status">{_html(row["status"])}</span><span class="op-action">{_html(row["action"])}</span></div>'
            "</div>"
        )
    st.markdown(
        f"""
        <section class="pipeline-ops">
          <div class="ops-header">
            <div><h2>流水线操作台</h2><p>把研究、组合、复核和纸面执行放在同一条可审计链路里；每一步都生成本地证据和下一步输入。</p></div>
            <span class="run-button">Run Daily Pipeline</span>
          </div>
          <div class="ops-grid">{''.join(cards)}</div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def _execution_gate_panel(card: dict) -> None:
    decision = str(card.get("decision", "")).lower()
    badge = "block" if decision == "reject" else "warn" if decision == "caution" else ""
    reasons = "".join(f"<li>{_html(reason)}</li>" for reason in card.get("reasons", [])[:4])
    st.markdown(
        f"""
        <section class="section decision-panel">
          <div class="section-header"><h2>纸面执行总判定</h2><span>{_html(card.get('action', ''))}</span></div>
          <div class="decision-body">
            <span class="badge {badge}">{_html(decision.upper())}</span>
            <strong>{_html(card.get('headline', ''))}</strong>
            <ul>{reasons}</ul>
          </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def _flow_map() -> None:
    rows = [
        ("Data", [("locked", "数据池", "CSI300 / CSI500 provider，行情、成交额、换手、证券主数据。"), ("audit", "公告事件", "公司事件、问询函、监管风险、ST 与停牌状态进入风险上下文。"), ("quality", "数据质量", "覆盖率、字段完整性、交易日一致性，失败则停止组合生成。")]),
        ("Research", [("registry", "因子池", "手工因子、JoinQuant 迁移因子、盘形事件因子统一进 registry。"), ("agent", "自动挖掘", "Codex CLI 只改候选表达式，contract 固定 provider、horizon、净化和评估器。"), ("purify", "因子净化", "MAD 去极值、z-score、rank 标准化、size proxy 中性化。")]),
        ("Decision", [("score", "单因子评价", "IC、Rank IC、分层收益、多空收益、换手和年度稳定性。"), ("gate", "组合门禁", "行业集中度、因子族数量、单一因子族贡献集中度、事件风险。"), ("committee", "专家复核", "LLM 投委会前置审查，reject 阻断，caution 降仓或人工确认。")]),
        ("Execution", [("target", "目标组合", "生成 target_portfolio，保留 top factor driver 和风险解释。"), ("paper", "纸面订单", "订单、模拟成交、手续费、涨跌停与停牌拒单。"), ("review", "对账复盘", "positions_expected、reconciliation、paper batch 复盘指标。")]),
    ]
    row_html = []
    for lane, steps in rows:
        step_html = "".join(
            f'<div class="step"><span class="badge">{_html(tag)}</span><b>{_html(title)}</b><p>{_html(desc)}</p></div>'
            for tag, title, desc in steps
        )
        row_html.append(f'<div class="flow-row"><div class="lane">{_html(lane)}</div><div class="steps">{step_html}</div></div>')
    st.markdown(
        f"""
        <section class="section">
          <div class="section-header"><h2>端到端流程地图</h2><span>from data to paper execution</span></div>
          <div class="flow">{''.join(row_html)}</div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def _right_rail(gate) -> None:
    checks = _gate_frame(gate.checks)
    gate_rows = []
    display_checks = [
        ("单票权重", "max_single_weight"),
        ("行业集中", "max_industry_weight"),
        ("因子族集中", "max_factor_family_concentration"),
        ("公告事件", "event_blocked_positions"),
    ]
    for label, check in display_checks:
        matched = checks.loc[checks["check"] == check] if not checks.empty and "check" in checks.columns else pd.DataFrame()
        status = "Pass" if matched.empty else str(matched.iloc[0].get("decision_level", matched.iloc[0].get("status", "pass"))).title()
        gate_rows.append(f'<div class="gate"><span>{_html(label)}</span><b>{_html(status)}</b></div>')
    st.markdown(
        f"""
        <aside class="right-rail">
          <section class="rail-panel"><h3>Portfolio Gate</h3>{''.join(gate_rows)}</section>
          <section class="rail-panel"><h3>Expert Review</h3><div class="decision">当前建议进入人工确认。组合不是被阻断，而是需要解释为何动量和反转暴露同时升高。</div></section>
          <section class="rail-panel"><h3>CLI Handoff</h3><div class="terminal">make autoresearch-codex-loop<br>make select-factors<br>make daily-signal<br>make target-portfolio<br>make exposure-attribution</div></section>
        </aside>
        """,
        unsafe_allow_html=True,
    )


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


def _html(value: object) -> str:
    text = str(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _style() -> None:
    st.markdown(
        """
        <style>
          :root {
            --fl-bg: #f4f2ec;
            --fl-ink: #1d2320;
            --fl-muted: #66706a;
            --fl-line: #cfd5cf;
            --fl-panel: #ffffff;
            --fl-panel-2: #eef2ef;
            --fl-green: #1f7a5a;
            --fl-red: #a84332;
            --fl-amber: #9b6a1f;
            --fl-blue: #2f5d8c;
            --fl-shadow: 0 18px 50px rgba(32, 43, 39, 0.12);
          }
          .stApp { background: var(--fl-bg); color: var(--fl-ink); }
          header[data-testid="stHeader"] { background: transparent; }
          div[data-testid="stToolbar"] { display: none; }
          .block-container {
            max-width: 1320px;
            padding-top: 2rem;
            padding-bottom: 3rem;
          }
          h1, h2, h3, h4, p, span, label, div { letter-spacing: 0; }
          h1 { font-size: 28px !important; line-height: 1.2 !important; font-weight: 780 !important; }
          h2, h3 { color: var(--fl-ink); }
          .stCaptionContainer, .stMarkdown p { color: var(--fl-muted); }
          section[data-testid="stSidebar"] {
            background: #18211e;
            border-right: 0;
            width: 240px !important;
          }
          section[data-testid="stSidebar"] * { color: #edf2ee; }
          section[data-testid="stSidebar"] h1 {
            font-size: 22px !important;
            font-weight: 760 !important;
            margin-top: 22px;
          }
          section[data-testid="stSidebar"] [role="radiogroup"] {
            display: grid;
            gap: 6px;
            margin-top: 18px;
          }
          section[data-testid="stSidebar"] label {
            border-radius: 6px;
            padding: 10px 8px;
          }
          section[data-testid="stSidebar"] label:has(input:checked) {
            background: #26342f;
            box-shadow: inset 3px 0 0 #55b08a;
          }
          section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
            color: #aebbb4;
            line-height: 1.5;
          }
          div[data-testid="stMetric"] {
            background: var(--fl-panel);
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            padding: 14px 15px;
            box-shadow: 0 8px 28px rgba(32, 43, 39, 0.07);
            overflow: hidden;
          }
          div[data-testid="stMetric"] label { color: var(--fl-muted); font-size: 12px; }
          div[data-testid="stMetricValue"] { font-size: 24px !important; line-height: 1.18; }
          div[data-testid="stMetricValue"] > div { overflow: hidden; text-overflow: ellipsis; }
          .stDataFrame {
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            overflow: hidden;
            background: var(--fl-panel);
          }
          .topbar {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 20px;
            margin-bottom: 18px;
          }
          .topbar h1 {
            margin: 0;
            font-size: 28px;
            line-height: 1.2;
            font-weight: 780;
          }
          .subtitle {
            margin-top: 7px;
            color: var(--fl-muted);
            font-size: 14px;
            line-height: 1.5;
            max-width: 780px;
          }
          .controls {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            justify-content: flex-end;
          }
          .controls span {
            border: 1px solid var(--fl-line);
            background: var(--fl-panel);
            color: var(--fl-ink);
            border-radius: 6px;
            padding: 9px 12px;
            font-size: 13px;
            font-weight: 650;
            white-space: nowrap;
          }
          .metrics {
            display: grid;
            grid-template-columns: repeat(5, minmax(130px, 1fr));
            gap: 12px;
            margin: 0 0 18px;
          }
          .metric {
            background: var(--fl-panel);
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            padding: 15px;
            box-shadow: 0 8px 28px rgba(32, 43, 39, 0.07);
            min-width: 0;
          }
          .metric label {
            display: block;
            color: var(--fl-muted);
            font-size: 12px;
            margin-bottom: 8px;
          }
          .metric strong {
            display: block;
            font-size: 24px;
            line-height: 1.15;
            font-weight: 760;
            overflow-wrap: anywhere;
          }
          .pipeline-ops, .section {
            background: var(--fl-panel);
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            box-shadow: var(--fl-shadow);
            margin-bottom: 18px;
            overflow: hidden;
          }
          .pipeline-ops {
            padding: 18px;
            display: grid;
            gap: 16px;
          }
          .ops-header, .section-header {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            gap: 18px;
          }
          .ops-header h2, .section-header h2 {
            margin: 0;
            font-size: 18px;
            font-weight: 760;
          }
          .ops-header p, .section-header span {
            margin: 5px 0 0;
            color: var(--fl-muted);
            font-size: 12px;
            line-height: 1.45;
          }
          .run-button {
            border: 1px solid #1c5f49;
            background: var(--fl-green);
            color: #ffffff;
            border-radius: 6px;
            padding: 10px 13px;
            font-size: 13px;
            font-weight: 750;
            white-space: nowrap;
          }
          .ops-grid {
            display: grid;
            grid-template-columns: repeat(3, minmax(180px, 1fr));
            gap: 12px;
          }
          .op-card {
            min-height: 128px;
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            background: #f8f9f6;
            padding: 12px;
            display: grid;
            grid-template-rows: auto auto 1fr auto;
            gap: 8px;
            min-width: 0;
          }
          .op-index {
            color: var(--fl-muted);
            font-size: 11px;
            font-weight: 760;
          }
          .op-card b {
            font-size: 14px;
            line-height: 1.3;
          }
          .op-card code {
            color: #415049;
            background: #e9eee9;
            border-radius: 5px;
            padding: 7px;
            font-size: 11px;
            line-height: 1.45;
            overflow-wrap: anywhere;
          }
          .op-footer, .gate {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 8px;
          }
          .op-status {
            font-size: 11px;
            font-weight: 760;
            color: var(--fl-green);
          }
          .op-action {
            border: 1px solid var(--fl-line);
            background: #ffffff;
            color: var(--fl-ink);
            border-radius: 6px;
            padding: 6px 8px;
            font-size: 11px;
            font-weight: 760;
          }
          .section-header { padding: 18px 18px 0; }
          .flow {
            padding: 18px;
            display: grid;
            gap: 12px;
          }
          .flow-row {
            display: grid;
            grid-template-columns: 116px 1fr;
            gap: 14px;
          }
          .lane {
            color: var(--fl-muted);
            font-size: 12px;
            font-weight: 700;
            padding-top: 14px;
            text-transform: uppercase;
          }
          .steps {
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 10px;
          }
          .step {
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            padding: 13px;
            background: var(--fl-panel-2);
            min-height: 108px;
            display: grid;
            align-content: start;
            gap: 8px;
          }
          .step b { font-size: 14px; }
          .step p {
            margin: 0;
            color: var(--fl-muted);
            font-size: 12px;
            line-height: 1.45;
          }
          .badge {
            width: fit-content;
            border-radius: 999px;
            padding: 3px 8px;
            font-size: 11px;
            font-weight: 750;
            background: #dce8e2;
            color: var(--fl-green);
          }
          .badge.warn {
            background: #f3e5c8;
            color: var(--fl-amber);
          }
          .badge.block {
            background: #f2d8d2;
            color: var(--fl-red);
          }
          .decision-panel .decision-body {
            padding: 18px;
            display: grid;
            gap: 10px;
          }
          .decision-body strong {
            font-size: 18px;
          }
          .decision-body ul {
            margin: 0;
            padding-left: 18px;
            color: var(--fl-muted);
            font-size: 13px;
            line-height: 1.5;
          }
          .right-rail {
            background: #edf0ec;
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            padding: 18px;
            display: grid;
            gap: 16px;
          }
          .rail-panel {
            background: var(--fl-panel);
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            padding: 16px;
            display: grid;
            gap: 13px;
          }
          .rail-panel h3 {
            margin: 0;
            font-size: 16px;
            font-weight: 760;
          }
          .gate {
            font-size: 13px;
          }
          .gate span { color: var(--fl-muted); }
          .decision {
            padding: 12px;
            border-radius: 8px;
            background: #f2e7ce;
            border: 1px solid #dfc891;
            color: #5e4216;
            line-height: 1.45;
            font-size: 13px;
            overflow-wrap: anywhere;
          }
          .terminal {
            background: #17201d;
            color: #d8e5de;
            border-radius: 8px;
            padding: 14px;
            font-family: "SFMono-Regular", Consolas, monospace;
            font-size: 12px;
            line-height: 1.55;
            overflow-wrap: anywhere;
          }
          @media (min-width: 1560px) {
            .ops-grid { grid-template-columns: repeat(6, minmax(0, 1fr)); }
          }
          @media (max-width: 1180px) {
            .metrics { grid-template-columns: repeat(3, minmax(130px, 1fr)); }
            .ops-grid, .steps { grid-template-columns: 1fr; }
            .flow-row { grid-template-columns: 1fr; }
          }
          @media (max-width: 820px) {
            .topbar, .ops-header, .section-header { display: grid; }
            .metrics { grid-template-columns: 1fr; }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
