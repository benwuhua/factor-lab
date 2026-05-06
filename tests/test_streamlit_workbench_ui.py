import unittest

from streamlit.testing.v1 import AppTest


APP_PATH = "app/streamlit_app.py"


class StreamlitWorkbenchUiTests(unittest.TestCase):
    def run_page(self, page: str) -> AppTest:
        app = AppTest.from_file(APP_PATH, default_timeout=20)
        app.query_params["page"] = page
        app.run(timeout=30)
        return app

    def assert_page_ok(self, app: AppTest, expected_text: str) -> None:
        if app.exception:
            messages = [str(item.value) for item in app.exception]
            self.fail(f"Streamlit page raised exceptions: {messages}")
        text = _app_text(app)
        self.assertIn(expected_text, text)

    def test_all_workbench_pages_render_without_exceptions(self):
        pages = [
            ("01 AI产业链", "AI产业链主题研究"),
            ("02 数据治理", "数据治理"),
            ("03 因子研究", "因子研究"),
            ("04 自动挖掘", "自动挖掘"),
            ("05 总览仪表盘", "投研业务流总览"),
            ("06 证据库", "证据库"),
        ]

        for page, expected_text in pages:
            with self.subTest(page=page):
                self.assert_page_ok(self.run_page(page), expected_text)

    def test_sidebar_navigation_exposes_all_pages(self):
        app = self.run_page("04 自动挖掘")
        labels = _sidebar_button_labels(app)

        for page in [
            "01 AI产业链",
            "02 数据治理",
            "03 因子研究",
            "04 自动挖掘",
            "05 总览仪表盘",
            "06 证据库",
        ]:
            with self.subTest(page=page):
                self.assertIn(page, labels)
        self.assertNotIn("07 纸面执行", labels)
        self.assertNotIn("05 组合门禁", labels)

    def test_sidebar_navigation_uses_native_buttons(self):
        app = self.run_page("04 自动挖掘")

        labels = _sidebar_button_labels(app)
        self.assertIn("04 自动挖掘", labels)
        self.assertIn("03 因子研究", labels)

    def test_factor_research_page_surfaces_multilane_smoke_and_actions(self):
        app = self.run_page("03 因子研究")
        text = _app_text(app)

        self.assertIn("最新 Smoke / 多车道结果", text)
        self.assertIn("信号模式与数据缺口", text)
        self.assertIn("Offensive", text)
        self.assertIn("Multilane Summary", text)
        self.assertIn("Run · 开始因子研究", _button_labels(app))
        self.assertIn("Smoke · 短窗多车道 smoke", _button_labels(app))
        self.assertIn("Run · 生成 approved 因子", _button_labels(app))

    def test_data_governance_page_surfaces_factor_data_gaps(self):
        app = self.run_page("02 数据治理")
        text = _app_text(app)

        self.assertIn("因子数据缺口", text)
        self.assertIn("revenue_growth_yoy", text)
        self.assertIn("Tushare 数据覆盖", text)
        self.assertIn("Tushare PIT 主数据", text)
        self.assertIn("财报披露事件", text)

    def test_autoresearch_page_surfaces_queue_multilane_and_loop_actions(self):
        app = self.run_page("04 自动挖掘")
        text = _app_text(app)
        labels = _button_labels(app)

        self.assertIn("Nightly 进度", text)
        self.assertIn("多车道结果", text)
        self.assertIn("Run · 多车道挖掘", labels)
        self.assertIn("Queue · 启动自动挖掘", labels)

    def test_default_page_is_ai_theme_signal_mode(self):
        app = AppTest.from_file(APP_PATH, default_timeout=20)
        app.run(timeout=30)

        self.assertIn("AI产业链主题研究", _app_text(app))

    def test_ai_theme_page_and_evidence_page_focus_on_signal_outputs(self):
        theme = self.run_page("01 AI产业链")
        evidence = self.run_page("06 证据库")

        self.assertIn("AI产业链主题研究", _app_text(theme))
        self.assertIn("A重点研究", _app_text(theme))
        self.assertIn("子链筛选", _app_text(theme))
        self.assertIn("非投资建议", _app_text(theme))
        self.assertIn("theme-scan", _app_text(theme))
        self.assertIn("刷新证据库", _app_text(evidence))
        self.assertNotIn("纸面执行动作", _app_text(theme))


def _button_labels(app: AppTest) -> list[str]:
    return [button.label for button in app.button]


def _sidebar_button_labels(app: AppTest) -> list[str]:
    return [button.label for button in app.sidebar.button]



def _app_text(app: AppTest) -> str:
    parts: list[str] = []
    for collection in [
        app.title,
        app.header,
        app.subheader,
        app.markdown,
        app.caption,
        app.info,
        app.warning,
        app.success,
        app.code,
        app.text,
    ]:
        parts.extend(str(item.value) for item in collection)
    parts.extend(_button_labels(app))
    for item in app.sidebar.markdown:
        parts.append(str(item.value))
    return "\n".join(parts)


if __name__ == "__main__":
    unittest.main()
