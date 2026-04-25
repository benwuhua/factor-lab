from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml


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
from qlib_factor_lab.workbench_tasks import (
    latest_workbench_task_runs,
    launch_workbench_task,
    load_workbench_task_detail,
    summarize_workbench_task_runs,
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

    if page == "02 数据治理":
        render_data_governance()
    elif page == "03 因子研究":
        render_factor_research()
    elif page == "04 自动挖掘":
        render_autoresearch_queue()
    elif page in {"05 组合门禁", "06 专家复核"}:
        render_portfolio_gate()
    elif page == "07 纸面执行":
        render_paper_execution()
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
        _render_workflow_task_buttons(pipeline, "dashboard")
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


def render_data_governance() -> None:
    snapshot = load_workbench_snapshot(ROOT)
    freshness = pd.DataFrame(snapshot.freshness)
    provider_rows = _provider_rows()
    quality_rows = _quality_rows(snapshot)

    st.markdown(
        _page_topbar_html(
            "数据治理",
            "固定沪深300和中证500数据边界，检查 provider、交易日、产物新鲜度和信号质量。",
            ["CSI300", "CSI500", "Quality", "Export"],
        ),
        unsafe_allow_html=True,
    )
    _metric_strip(
        [
            ("Provider Groups", len(provider_rows)),
            ("Fresh Artifacts", _fresh_count(freshness, "ready")),
            ("Stale Artifacts", _fresh_count(freshness, "stale")),
            ("Missing Artifacts", _fresh_count(freshness, "missing")),
            ("Latest Run", snapshot.latest_run_dir.name if snapshot.latest_run_dir else "n/a"),
        ]
    )

    main_col, rail_col = st.columns([3.1, 1.05], gap="large")
    with main_col:
        st.markdown(_section_header_html("固定数据边界", "provider contract"), unsafe_allow_html=True)
        st.markdown(_workflow_card_grid_html(provider_rows), unsafe_allow_html=True)

        st.markdown(_section_header_html("产物新鲜度", "artifact freshness"), unsafe_allow_html=True)
        if freshness.empty:
            st.info("暂无可检查的产物。")
        else:
            freshness = freshness.copy()
            freshness["灯号"] = freshness["status"].map({"ready": "green", "stale": "yellow", "missing": "red"}).fillna("gray")
            st.dataframe(
                freshness.loc[:, ["灯号", "label", "status", "age_hours", "path"]],
                use_container_width=True,
                hide_index=True,
            )

        st.markdown(_section_header_html("质量检查动作", "quality gate"), unsafe_allow_html=True)
        st.markdown(_workflow_card_grid_html(quality_rows), unsafe_allow_html=True)
        _render_workflow_task_buttons(quality_rows, "data-quality")
    with rail_col:
        st.markdown(
            '<aside class="right-rail detail-rail">'
            + _detail_card_html(
                "Data Boundary",
                [("universe", "CSI300 / CSI500"), ("provider", "locked"), ("mode", "read-only")],
                note="数据治理页只展示和审计边界，不在 UI 中直接修改 provider。",
            )
            + _detail_card_html(
                "Freshness",
                [
                    ("ready", _fresh_count(freshness, "ready")),
                    ("stale", _fresh_count(freshness, "stale")),
                    ("missing", _fresh_count(freshness, "missing")),
                ],
                note="超过 freshness SLA 的产物会在后续组合环节被视为待复核输入。",
            )
            + '<section class="rail-panel"><h3>CLI Handoff</h3><div class="terminal">make check-env<br>make check-data-quality<br>make daily-signal</div></section>'
            + "</aside>",
            unsafe_allow_html=True,
        )


def render_factor_research() -> None:
    queue = load_autoresearch_queue(ROOT)
    approved = _approved_factor_rows()
    diagnostics = _latest_single_factor_diagnostics()
    summary = summarize_autoresearch_queue(queue)
    family_counts = approved["family"].fillna("unknown").astype(str).value_counts() if not approved.empty and "family" in approved.columns else pd.Series(dtype=int)

    st.markdown(
        _page_topbar_html(
            "因子研究",
            "把候选因子、迁移因子和 nightly 挖掘结果沉淀到 approved 因子清单，再进入单因子评价和组合构建。",
            ["Registry", "Approved", "Diagnostics", "Export"],
        ),
        unsafe_allow_html=True,
    )
    _metric_strip(
        [
            ("Approved", len(approved)),
            ("Families", int(family_counts.size)),
            ("Nightly Review", summary["review"]),
            ("Diagnostics", len(diagnostics)),
            ("Registry", "factors/registry.yaml"),
        ]
    )

    main_col, rail_col = st.columns([3.1, 1.05], gap="large")
    with main_col:
        st.markdown(_section_header_html("研究动作", "factor workflow"), unsafe_allow_html=True)
        factor_rows = _factor_research_rows()
        st.markdown(_workflow_card_grid_html(factor_rows), unsafe_allow_html=True)
        _render_workflow_task_buttons(factor_rows, "factor-research")

        st.markdown(_section_header_html("Approved 因子清单", "governed factor set"), unsafe_allow_html=True)
        if approved.empty:
            st.info("还没有 approved_factors.yaml。")
        else:
            st.dataframe(approved, use_container_width=True, hide_index=True)

        chart_cols = st.columns(2)
        with chart_cols[0]:
            st.subheader("因子族分布")
            if family_counts.empty:
                st.info("暂无因子族字段。")
            else:
                st.bar_chart(family_counts)
        with chart_cols[1]:
            st.subheader("单因子诊断")
            if diagnostics.empty:
                st.info("暂无 single_factor_diagnostics CSV。")
            else:
                keep = [column for column in ["factor", "status", "family", "neutral_rank_ic_mean_h20", "decision"] if column in diagnostics.columns]
                st.dataframe(diagnostics.loc[:, keep] if keep else diagnostics, use_container_width=True, hide_index=True)
    with rail_col:
        st.markdown(
            '<aside class="right-rail detail-rail">'
            + _detail_card_html(
                "Research Queue",
                [
                    ("review", summary.get("review", 0)),
                    ("discard", summary.get("discard_candidate", 0)),
                    ("crash", summary.get("crash", 0)),
                ],
                note="因子研究页只展示通过治理后的候选，不直接把 nightly 结果塞进组合。",
            )
            + _detail_card_html(
                "Factor Families",
                [("count", int(family_counts.size)), ("top", str(family_counts.index[0]) if not family_counts.empty else "n/a")],
                note="后续 portfolio gate 会检查因子族集中度，避免组合只押单一量价逻辑。",
            )
            + '<section class="rail-panel"><h3>CLI Handoff</h3><div class="terminal">make select-factors<br>make daily-signal<br>make exposure-attribution</div></section>'
            + "</aside>",
            unsafe_allow_html=True,
        )


def render_portfolio_gate() -> None:
    st.markdown(
        _page_topbar_html(
            "组合门禁",
            "组合进入纸面执行前，先通过暴露、事件、流动性和专家复核的可审计检查。",
            ["CSI300", "CSI500", "Gate", "Export"],
        ),
        unsafe_allow_html=True,
    )
    latest = find_latest_target_portfolio(ROOT)
    if latest is None:
        st.warning("还没有找到 target_portfolio CSV。先运行 make target-portfolio。")
        return

    portfolio = pd.read_csv(latest)
    gate = build_portfolio_gate_explanation(
        portfolio,
        risk_config=load_risk_config_dict(ROOT),
        factor_family_map=load_factor_family_map_safe(ROOT),
    )
    expert = _latest_expert_review()
    pretrade = build_pretrade_review(portfolio)
    execution_card = load_execution_gate_card(ROOT)
    review_items = build_gate_review_items(gate.checks)
    pretrade_blocks = int((pretrade["status"].astype(str) == "reject").sum()) if not pretrade.empty else 0

    _metric_strip(
        [
            ("Gate Decision", gate.decision.upper()),
            ("Expert Review", str(expert["decision"]).upper()),
            ("Watchlist", len(expert["watchlist"])),
            ("Pretrade Blocks", pretrade_blocks),
            ("Target Rows", len(portfolio)),
        ]
    )

    main_col, rail_col = st.columns([3.1, 1.05], gap="large")
    with main_col:
        _execution_gate_panel(execution_card)
        portfolio_rows = _portfolio_gate_rows()
        st.markdown(_section_header_html("组合门禁动作", "portfolio workflow"), unsafe_allow_html=True)
        st.markdown(_workflow_card_grid_html(portfolio_rows), unsafe_allow_html=True)
        _render_workflow_task_buttons(portfolio_rows, "portfolio-gate")

        st.markdown(_section_header_html("专家复核结构化摘要", "committee pre-check"), unsafe_allow_html=True)
        st.markdown(
            _detail_card_html(
                "复核结论",
                [
                    ("status", expert["status"]),
                    ("decision", expert["decision"]),
                    ("watchlist", len(expert["watchlist"])),
                ],
                note=expert["summary"] or "暂无专家复核摘要。",
            ),
            unsafe_allow_html=True,
        )
        if expert["risk_notes"]:
            st.caption(expert["risk_notes"])
        if expert["watchlist"]:
            st.dataframe(pd.DataFrame({"instrument": expert["watchlist"]}), use_container_width=True, hide_index=True)

        st.markdown(_section_header_html("为什么被 caution / reject", "gate evidence"), unsafe_allow_html=True)
        st.dataframe(_gate_frame(gate.checks), use_container_width=True, hide_index=True)
        if review_items.empty:
            st.success("当前 gate 没有失败项。")
        else:
            st.markdown(
                _section_header_html("前置复核动作", "caution 降仓或人工确认，reject 阻断"),
                unsafe_allow_html=True,
            )
            st.dataframe(review_items, use_container_width=True, hide_index=True)

        st.markdown(
            _section_header_html("交易前检查", "announcement, limit, suspension, liquidity"),
            unsafe_allow_html=True,
        )
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
    with rail_col:
        _portfolio_detail_rail(latest, gate, expert, pretrade)


def render_paper_execution() -> None:
    snapshot = load_workbench_snapshot(ROOT)
    latest = find_latest_run_dir(ROOT)
    orders = _read_latest_run_csv("orders.csv")
    fills = _read_latest_run_csv("fills.csv")
    positions = _read_latest_run_csv("positions_expected.csv")
    gate_card = load_execution_gate_card(ROOT)

    st.markdown(
        _page_topbar_html(
            "纸面执行",
            "把目标组合转换为纸面订单，经过涨跌停、停牌、流动性和专家门禁后生成可复盘执行包。",
            ["Orders", "Fills", "Positions", "Export"],
        ),
        unsafe_allow_html=True,
    )
    _metric_strip(
        [
            ("Execution Gate", str(gate_card.get("decision", "")).upper()),
            ("Orders", len(orders)),
            ("Fills", len(fills)),
            ("Expected Pos", len(positions)),
            ("Latest Run", latest.name if latest else "n/a"),
        ]
    )

    main_col, rail_col = st.columns([3.1, 1.05], gap="large")
    with main_col:
        _execution_gate_panel(gate_card)
        st.markdown(_section_header_html("纸面执行动作", "paper workflow"), unsafe_allow_html=True)
        paper_rows = _paper_execution_rows()
        st.markdown(_workflow_card_grid_html(paper_rows), unsafe_allow_html=True)
        _render_workflow_task_buttons(paper_rows, "paper-execution")

        tab_orders, tab_fills, tab_positions = st.tabs(["Orders", "Fills", "Expected Positions"])
        with tab_orders:
            if orders.empty:
                st.info("最近 run 里还没有 orders.csv。")
            else:
                st.dataframe(orders, use_container_width=True, hide_index=True)
        with tab_fills:
            if fills.empty:
                st.info("最近 run 里还没有 fills.csv。")
            else:
                st.dataframe(fills, use_container_width=True, hide_index=True)
        with tab_positions:
            if positions.empty:
                st.info("最近 run 里还没有 positions_expected.csv。")
            else:
                st.dataframe(positions, use_container_width=True, hide_index=True)

        pipeline = build_research_pipeline_status(ROOT)
        st.markdown(_section_header_html("执行包状态", "paper bundle"), unsafe_allow_html=True)
        st.dataframe(pipeline, use_container_width=True, hide_index=True)
    with rail_col:
        st.markdown(
            '<aside class="right-rail detail-rail">'
            + _detail_card_html(
                "Paper Bundle",
                [
                    ("run", latest.name if latest else "n/a"),
                    ("orders", len(orders)),
                    ("fills", len(fills)),
                ],
                note="纸面执行只生成模拟订单和对账产物，不代表真实交易委托。",
            )
            + _detail_card_html(
                "Execution Gate",
                [
                    ("decision", gate_card.get("decision", "")),
                    ("action", gate_card.get("action", "")),
                    ("freshness", "ready" if snapshot.latest_run_dir else "missing"),
                ],
                note="reject 阻断纸面执行，caution 要求人手确认后继续。",
            )
            + '<section class="rail-panel"><h3>CLI Handoff</h3><div class="terminal">make paper-orders<br>make reconcile-account<br>make paper-batch</div></section>'
            + "</aside>",
            unsafe_allow_html=True,
        )


def render_autoresearch_queue() -> None:
    st.markdown(
        _page_topbar_html(
            "自动挖掘",
            "Codex CLI 只改候选表达式，contract 固定 provider、horizon、净化和评估器。",
            ["expression", "pattern", "combo", "Export"],
        ),
        unsafe_allow_html=True,
    )
    queue = load_autoresearch_queue(ROOT)
    if queue.empty:
        st.warning("还没有 expression_results.tsv。先运行 make autoresearch-expression 或 autoresearch-codex-loop。")
        return

    summary = summarize_autoresearch_queue(queue)
    primary = queue["primary_metric"].dropna().max() if "primary_metric" in queue.columns else float("nan")
    _metric_strip(
        [
            ("Review", summary["review"]),
            ("Discard", summary["discard_candidate"]),
            ("Crash", summary["crash"]),
            ("Best Primary", f"{primary:.4f}" if pd.notna(primary) else "n/a"),
            ("Candidates", len(queue)),
        ]
    )

    status = st.multiselect(
        "状态过滤",
        options=sorted(queue["status"].dropna().astype(str).unique()),
        default=["review"] if "review" in set(queue["status"].astype(str)) else [],
    )
    filtered = queue[queue["status"].astype(str).isin(status)] if status else queue
    selected = ""
    selected_artifact = ""

    main_col, rail_col = st.columns([3.1, 1.05], gap="large")
    with main_col:
        auto_rows = _autoresearch_rows()
        st.markdown(_section_header_html("自动挖掘动作", "controlled research loop"), unsafe_allow_html=True)
        st.markdown(_workflow_card_grid_html(auto_rows), unsafe_allow_html=True)
        _render_workflow_task_buttons(auto_rows, "autoresearch")

        st.markdown(_section_header_html("研究队列", "nightly ledger"), unsafe_allow_html=True)
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

        st.markdown(
            _section_header_html("候选对比", "primary metric vs complexity"),
            unsafe_allow_html=True,
        )
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
    with rail_col:
        _autoresearch_detail_rail(summary, selected, selected_artifact)


def _page_topbar_html(title: str, subtitle: str, controls: list[str]) -> str:
    control_html = "".join(f"<span>{_html(control)}</span>" for control in controls)
    return (
        '<div class="topbar detail-topbar">'
        "<div>"
        f"<h1>{_html(title)}</h1>"
        f'<div class="subtitle">{_html(subtitle)}</div>'
        "</div>"
        f'<div class="controls">{control_html}</div>'
        "</div>"
    )


def _section_header_html(title: str, caption: str) -> str:
    return (
        '<section class="section compact-section">'
        f'<div class="section-header"><h2>{_html(title)}</h2><span>{_html(caption)}</span></div>'
        "</section>"
    )


def _detail_card_html(title: str, rows: list[tuple[str, object]], *, note: str = "") -> str:
    row_html = "".join(f"<div><span>{_html(label)}</span><b>{_html(value)}</b></div>" for label, value in rows)
    note_html = f'<p class="detail-note">{_html(note)}</p>' if note else ""
    return (
        '<section class="detail-card">'
        f"<h3>{_html(title)}</h3>"
        f'<div class="detail-kv">{row_html}</div>'
        f"{note_html}"
        "</section>"
    )


def _portfolio_detail_rail(latest: Path, gate, expert: dict, pretrade: pd.DataFrame) -> None:
    gate_counts = _status_counts(gate.checks)
    pretrade_counts = _status_counts(pretrade)
    st.markdown(
        '<aside class="right-rail detail-rail">'
        + _detail_card_html(
            "Portfolio Gate",
            [
                ("decision", gate.decision.upper()),
                ("failed checks", gate_counts.get("fail", 0)),
                ("pretrade reject", pretrade_counts.get("reject", 0)),
            ],
            note="reject 自动阻断，caution 进入降仓或人工确认。",
        )
        + _detail_card_html(
            "Expert Review",
            [
                ("status", expert.get("status", "")),
                ("decision", expert.get("decision", "")),
                ("watchlist", len(expert.get("watchlist", []))),
            ],
            note=str(expert.get("risk_notes") or expert.get("summary") or "暂无专家复核输出。")[:160],
        )
        + _detail_card_html(
            "Target",
            [("file", latest.name), ("path", latest.parent.name)],
            note="组合明细保留 top factor driver、行业、事件和交易前检查证据。",
        )
        + '<section class="rail-panel"><h3>CLI Handoff</h3><div class="terminal">make target-portfolio<br>make exposure-attribution<br>make paper-orders</div></section>'
        + "</aside>",
        unsafe_allow_html=True,
    )


def _autoresearch_detail_rail(summary: dict[str, int], selected: str, selected_artifact: str) -> None:
    st.markdown(
        '<aside class="right-rail detail-rail">'
        + _detail_card_html(
            "Queue Status",
            [
                ("review", summary.get("review", 0)),
                ("discard", summary.get("discard_candidate", 0)),
                ("crash", summary.get("crash", 0)),
            ],
            note="ledger 只记录受控候选，避免 nightly 重复探索同一类表达式。",
        )
        + _detail_card_html(
            "Selected Candidate",
            [
                ("name", _short_text(selected or "n/a", 12)),
                ("artifact", _short_text(Path(str(selected_artifact)).name if selected_artifact else "n/a", 12)),
            ],
            note="详情区优先看中性化 IC、年度稳定性和重复因子簇。",
        )
        + '<section class="rail-panel"><h3>CLI Handoff</h3><div class="terminal">make autoresearch-codex-loop<br>make autoresearch-review<br>make select-factors</div></section>'
        + "</aside>",
        unsafe_allow_html=True,
    )


def _status_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame.empty or "status" not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame["status"].fillna("").astype(str).value_counts().items()}


def _short_text(value: object, limit: int) -> str:
    text = str(value)
    return text if len(text) <= limit else f"{text[: max(limit - 3, 1)]}..."


def _workflow_card_grid_html(rows: list[dict[str, object]]) -> str:
    cards = []
    for row in rows:
        cards.append(
            '<div class="op-card">'
            f'<span class="op-index">{_html(row.get("step", ""))}</span>'
            f'<b>{_html(row.get("title", ""))}</b>'
            f'<code>{_html(row.get("command", ""))}</code>'
            f'<div class="op-footer"><span class="op-status">{_html(row.get("status", ""))}</span><span class="op-action">{_html(row.get("action", ""))}</span></div>'
            "</div>"
        )
    return f'<div class="ops-grid workflow-grid">{"".join(cards)}</div>'


def _render_workflow_task_buttons(rows: list[dict[str, object]], key_prefix: str) -> None:
    task_rows = [row for row in rows if row.get("task_id")]
    if not task_rows:
        return
    st.markdown('<div class="task-button-row">', unsafe_allow_html=True)
    cols = st.columns(min(len(task_rows), 4))
    for index, row in enumerate(task_rows):
        task_id = str(row["task_id"])
        label = f'{row.get("action", "Run")} · {row.get("title", task_id)}'
        with cols[index % len(cols)]:
            if st.button(label, key=f"task-{key_prefix}-{task_id}", use_container_width=True):
                record = launch_workbench_task(ROOT, task_id)
                st.success(f"已启动后台任务: {task_id}")
                st.caption(f"manifest: {record.manifest_path}")
    st.markdown("</div>", unsafe_allow_html=True)
    _render_task_monitor(key_prefix)


@st.fragment(run_every="10s")
def _render_task_monitor(key_prefix: str) -> None:
    recent = latest_workbench_task_runs(ROOT, limit=5)
    if not recent:
        return
    summary = summarize_workbench_task_runs(recent)
    st.markdown(_task_status_cards_html(summary), unsafe_allow_html=True)
    running_count = summary.get("queued", 0) + summary.get("running", 0)
    if running_count:
        st.progress(min(running_count / max(len(recent), 1), 1.0), text=f"{running_count} 个后台任务仍在运行或排队")
    if st.button("刷新任务状态", key=f"refresh-tasks-{key_prefix}", use_container_width=True):
        st.rerun(scope="fragment")
    with st.expander("最近后台任务", expanded=bool(running_count)):
        display = pd.DataFrame(recent)
        keep = [column for column in ["created_at", "task_id", "status", "returncode", "run_dir"] if column in display.columns]
        st.dataframe(display.loc[:, keep], use_container_width=True, hide_index=True)
        latest = recent[0]
        if latest.get("log_tail"):
            st.caption(f"latest log: {latest.get('task_id')}")
            st.code(str(latest["log_tail"]), language="text")
        selected_run = st.selectbox(
            "查看任务详情",
            recent,
            format_func=_task_run_option_label,
            key=f"task-detail-{key_prefix}",
        )
        if selected_run:
            detail = load_workbench_task_detail(str(selected_run.get("run_dir", "")))
            left, right = st.columns([0.9, 1.1])
            with left:
                st.caption("manifest")
                st.json(detail["manifest"], expanded=False)
            with right:
                st.caption(f"full log · {detail['log_line_count']} lines")
                st.code(detail["log"] or "log not written yet", language="text")


def _task_status_cards_html(summary: dict[str, int]) -> str:
    items = []
    for status in ["queued", "running", "succeeded", "failed"]:
        klass = "bad" if status == "failed" and summary.get(status, 0) else "active" if status in {"queued", "running"} and summary.get(status, 0) else ""
        items.append(
            f'<div class="task-status {klass}"><label>{_html(status)}</label><strong>{_html(summary.get(status, 0))}</strong></div>'
        )
    return f'<section class="task-status-grid">{"".join(items)}</section>'


def _task_run_option_label(row: dict[str, object]) -> str:
    run_name = Path(str(row.get("run_dir", "n/a"))).name
    status = str(row.get("status", "unknown") or "unknown")
    returncode = row.get("returncode")
    code = "pending" if returncode is None else f"rc={returncode}"
    return f"{run_name} · {status} · {code}"


def _provider_rows() -> list[dict[str, object]]:
    return [
        {
            "step": "CSI300",
            "title": "沪深300 provider",
            "command": "configs/provider_csi300_current.yaml",
            "status": _exists_status(ROOT / "configs/provider_csi300_current.yaml"),
            "action": "Open",
        },
        {
            "step": "CSI500",
            "title": "中证500 provider",
            "command": "configs/provider_current.yaml",
            "status": _exists_status(ROOT / "configs/provider_current.yaml"),
            "action": "Open",
        },
        {
            "step": "RISK",
            "title": "组合风险合同",
            "command": "configs/risk.yaml",
            "status": _exists_status(ROOT / "configs/risk.yaml"),
            "action": "Review",
        },
    ]


def _quality_rows(snapshot) -> list[dict[str, object]]:
    return [
        {"step": "01 ENV", "title": "检查 provider 环境", "command": "make check-env", "status": "ready", "action": "Run", "task_id": "check-env"},
        {"step": "02 SIGNAL", "title": "生成当日信号", "command": "make daily-signal", "status": "ready", "action": "Run", "task_id": "daily-signal"},
        {"step": "03 QUALITY", "title": "检查信号质量", "command": "make check-data-quality", "status": "guarded", "action": "Gate", "task_id": "check-data-quality"},
    ]


def _factor_research_rows() -> list[dict[str, object]]:
    return [
        {"step": "01 REGISTRY", "title": "维护候选因子池", "command": "factors/registry.yaml", "status": _exists_status(ROOT / "factors/registry.yaml"), "action": "Open"},
        {"step": "02 SELECT", "title": "生成 approved 因子", "command": "make select-factors", "status": _exists_status(ROOT / "reports/approved_factors.yaml"), "action": "Run", "task_id": "select-factors"},
        {"step": "03 SIGNAL", "title": "构建解释型信号", "command": "make daily-signal", "status": "ready", "action": "Run", "task_id": "daily-signal"},
    ]


def _autoresearch_rows() -> list[dict[str, object]]:
    return [
        {"step": "01 LOOP", "title": "启动自动挖掘", "command": "make autoresearch-codex-loop", "status": "review", "action": "Queue", "task_id": "autoresearch-codex-loop"},
        {"step": "02 REVIEW", "title": "复核挖掘结果", "command": "make autoresearch-review", "status": "ready", "action": "Review", "task_id": "autoresearch-review"},
        {"step": "03 SELECT", "title": "沉淀 approved 因子", "command": "make select-factors", "status": "governed", "action": "Run", "task_id": "select-factors"},
    ]


def _portfolio_gate_rows() -> list[dict[str, object]]:
    return [
        {"step": "01 TARGET", "title": "生成目标组合", "command": "make target-portfolio", "status": "ready", "action": "Run", "task_id": "target-portfolio"},
        {"step": "02 ATTR", "title": "暴露归因", "command": "make exposure-attribution", "status": "ready", "action": "Review", "task_id": "exposure-attribution"},
        {"step": "03 PAPER", "title": "生成纸面订单", "command": "make paper-orders", "status": "guarded", "action": "Stage", "task_id": "paper-orders"},
    ]


def _paper_execution_rows() -> list[dict[str, object]]:
    return [
        {"step": "01 ORDERS", "title": "生成纸面订单", "command": "make paper-orders", "status": "ready", "action": "Stage", "task_id": "paper-orders"},
        {"step": "02 RECON", "title": "账户对账", "command": "make reconcile-account", "status": "ready", "action": "Check", "task_id": "reconcile-account"},
        {"step": "03 BATCH", "title": "滚动纸面批测", "command": "make paper-batch", "status": "optional", "action": "Run", "task_id": "paper-batch"},
    ]


def _approved_factor_rows() -> pd.DataFrame:
    path = ROOT / "reports/approved_factors.yaml"
    if not path.exists():
        return pd.DataFrame(columns=["name", "family", "status", "direction", "review_notes"])
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows = []
    for item in data.get("approved_factors", []) or []:
        rows.append(
            {
                "name": item.get("name", ""),
                "family": item.get("family", ""),
                "status": item.get("status", "approved"),
                "direction": item.get("direction", ""),
                "review_notes": item.get("review_notes", ""),
            }
        )
    return pd.DataFrame(rows)


def _latest_single_factor_diagnostics() -> pd.DataFrame:
    candidates = sorted((ROOT / "reports").glob("single_factor_diagnostics*.csv"), key=lambda path: path.stat().st_mtime)
    if not candidates:
        return pd.DataFrame()
    return pd.read_csv(candidates[-1])


def _read_latest_run_csv(filename: str) -> pd.DataFrame:
    latest = find_latest_run_dir(ROOT)
    path = latest / filename if latest is not None else None
    return pd.read_csv(path) if path is not None and path.exists() else pd.DataFrame()


def _fresh_count(frame: pd.DataFrame, status: str) -> int:
    if frame.empty or "status" not in frame.columns:
        return 0
    return int((frame["status"].astype(str) == status).sum())


def _exists_status(path: Path) -> str:
    return "ready" if path.exists() else "missing"


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


def _pipeline_rows(gate_decision: str) -> list[dict[str, object]]:
    return [
        {"step": "01 DATA", "title": "检查数据与交易日", "command": "make check-env", "status": "ready", "action": "Run", "task_id": "check-env"},
        {"step": "02 RESEARCH", "title": "启动候选因子挖掘", "command": "make autoresearch-codex-loop", "status": "review", "action": "Queue", "task_id": "autoresearch-codex-loop"},
        {"step": "03 GOVERN", "title": "生成 approved 因子", "command": "make select-factors", "status": "governed", "action": "Open", "task_id": "select-factors"},
        {"step": "04 SIGNAL", "title": "构建当日信号", "command": "make daily-signal", "status": "built", "action": "Run", "task_id": "daily-signal"},
        {"step": "05 PORTFOLIO", "title": "组合与暴露门禁", "command": "make target-portfolio", "status": gate_decision, "action": "Review", "task_id": "target-portfolio"},
        {"step": "06 PAPER", "title": "纸面订单与对账", "command": "make paper-orders", "status": "ready", "action": "Stage", "task_id": "paper-orders"},
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
          .task-button-row {
            display: block;
            margin: -4px 0 12px;
          }
          .task-status-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 10px;
            margin: 8px 0 12px;
          }
          .task-status {
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            background: #f8f9f6;
            padding: 10px 12px;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 10px;
          }
          .task-status label {
            color: var(--fl-muted);
            font-size: 12px;
            white-space: nowrap;
          }
          .task-status strong {
            font-size: 18px;
            color: var(--fl-ink);
          }
          .task-status.active {
            border-color: #9ec9b7;
            background: #ecf5f0;
          }
          .task-status.bad {
            border-color: #e1b2a9;
            background: #f8e8e4;
          }
          .section-header { padding: 18px 18px 0; }
          .detail-topbar {
            padding-bottom: 2px;
            border-bottom: 1px solid rgba(207, 213, 207, 0.75);
          }
          .compact-section {
            box-shadow: none;
            margin: 18px 0 10px;
            background: #f8f9f6;
          }
          .compact-section .section-header {
            padding: 14px 16px;
          }
          .detail-card {
            background: var(--fl-panel);
            border: 1px solid var(--fl-line);
            border-radius: 8px;
            padding: 16px;
            margin-bottom: 14px;
            box-shadow: 0 8px 28px rgba(32, 43, 39, 0.07);
            display: grid;
            gap: 12px;
            min-width: 0;
          }
          .detail-card h3 {
            margin: 0;
            font-size: 16px;
            font-weight: 760;
          }
          .detail-kv {
            display: grid;
            gap: 8px;
          }
          .detail-kv div {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            border-top: 1px solid #edf0ec;
            padding-top: 8px;
            font-size: 13px;
            min-width: 0;
          }
          .detail-kv span {
            color: var(--fl-muted);
            flex: 0 0 auto;
            white-space: nowrap;
          }
          .detail-kv b {
            color: var(--fl-ink);
            flex: 1 1 auto;
            text-align: right;
            overflow-wrap: break-word;
            word-break: normal;
            min-width: 0;
          }
          .detail-note {
            margin: 0;
            color: var(--fl-muted);
            font-size: 12px;
            line-height: 1.45;
          }
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
            box-sizing: border-box;
            max-width: 100%;
            overflow: hidden;
          }
          .detail-rail {
            position: sticky;
            top: 18px;
            align-self: start;
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
            div[data-testid="stHorizontalBlock"]:has(aside.right-rail) {
              display: grid !important;
              grid-template-columns: 1fr !important;
              gap: 18px !important;
            }
            div[data-testid="stHorizontalBlock"]:has(aside.right-rail) > div {
              min-width: 0 !important;
              max-width: 100% !important;
              width: 100% !important;
              flex: 1 1 auto !important;
            }
            .detail-rail {
              position: static;
            }
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
