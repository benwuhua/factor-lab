#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterator
from urllib.parse import quote_plus
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]
APP_PATH = ROOT / "app" / "streamlit_app.py"


def main() -> int:
    args = parse_args()
    try:
        from playwright.sync_api import expect, sync_playwright
    except ImportError:
        print(
            "Playwright is not installed. Install optional E2E dependencies with:\n"
            "  .venv/bin/pip install playwright\n"
            "  .venv/bin/python -m playwright install chromium",
            file=sys.stderr,
        )
        return 2

    server = None
    if args.start_server and not _server_ready(args.base_url):
        server = _start_streamlit(args.base_url)
        _wait_for_server(args.base_url, timeout_sec=args.startup_timeout)

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=not args.headed)
            page = browser.new_page(viewport={"width": 1440, "height": 1100})
            _run_workbench_checks(page, args.base_url, expect)
            browser.close()
    finally:
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=8)
            except subprocess.TimeoutExpired:
                server.kill()
    print("workbench e2e passed")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run browser E2E checks for the Streamlit workbench.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8501")
    parser.add_argument("--headed", action="store_true", help="Show the browser while running checks.")
    parser.add_argument("--no-start-server", dest="start_server", action="store_false", help="Require an existing Streamlit server.")
    parser.add_argument("--startup-timeout", type=float, default=30.0)
    parser.set_defaults(start_server=True)
    return parser.parse_args()


def _run_workbench_checks(page, base_url: str, expect) -> None:
    _open_page(page, base_url, "04 自动挖掘")
    _expand_sidebar_if_collapsed(page)
    expect(page.get_by_role("heading", name="自动挖掘", exact=True)).to_be_visible()
    expect(page.get_by_role("button", name="Queue · 启动自动挖掘")).to_be_visible()

    _click_nav_button(page, "03 因子研究", expect)
    expect(page).to_have_url(f"{base_url}/?page={quote_plus('03 因子研究')}")
    expect(page.get_by_role("heading", name="因子研究", exact=True)).to_be_visible()
    expect(page.get_by_role("button", name="Smoke · 短窗多车道 smoke")).to_be_visible()
    expect(page.get_by_role("button", name="Run · 生成 approved 因子")).to_be_visible()

    _click_nav_button(page, "08 证据库", expect)
    expect(page).to_have_url(f"{base_url}/?page={quote_plus('08 证据库')}")
    expect(page.get_by_role("heading", name="证据库", exact=True)).to_be_visible()
    refresh_evidence = page.get_by_role("button", name="Run · 刷新证据库")
    expect(refresh_evidence).to_be_visible()

    _click_nav_button(page, "04 自动挖掘", expect)
    queue_button = page.get_by_role("button", name="Queue · 启动自动挖掘")
    expect(queue_button).to_be_visible()
    refresh_tasks = page.get_by_role("button", name="刷新任务状态")
    expect(refresh_tasks).to_be_visible()
    refresh_tasks.click()
    expect(page.get_by_role("button", name="Queue · 启动自动挖掘")).to_be_visible()

    _click_nav_button(page, "05 组合门禁", expect)
    expect(page.get_by_text("为什么被 caution / reject")).to_be_visible()
    expect(page.get_by_text("公告证据复核")).to_be_visible()
    expect(page.get_by_role("heading", name="组合门禁", exact=True)).to_be_visible()


def _open_page(page, base_url: str, page_name: str) -> None:
    page.goto(f"{base_url}/?page={quote_plus(page_name)}")
    page.wait_for_load_state("networkidle")


def _click_nav_button(page, page_name: str, expect) -> None:
    _expand_sidebar_if_collapsed(page)
    nav_option = page.get_by_role("button", name=page_name)
    expect(nav_option).to_be_visible()
    nav_option.click()
    page.wait_for_load_state("networkidle")


def _expand_sidebar_if_collapsed(page) -> None:
    expand = page.get_by_role("button", name="keyboard_double_arrow_right")
    if expand.count() == 1 and expand.is_visible():
        expand.click()
        page.wait_for_load_state("networkidle")


def _start_streamlit(base_url: str) -> subprocess.Popen:
    host, port = _host_port(base_url)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src")
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            str(APP_PATH),
            "--server.address",
            host,
            "--server.port",
            str(port),
            "--server.headless",
            "true",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _wait_for_server(base_url: str, timeout_sec: float) -> None:
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if _server_ready(base_url):
            return
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for Streamlit at {base_url}")


def _server_ready(base_url: str) -> bool:
    try:
        with urlopen(base_url, timeout=1.0) as response:
            return 200 <= response.status < 500
    except Exception:
        return False


def _host_port(base_url: str) -> tuple[str, int]:
    prefix = "http://"
    if not base_url.startswith(prefix):
        raise ValueError("--base-url must start with http://")
    host_port = base_url[len(prefix) :].split("/", 1)[0]
    if ":" not in host_port:
        return host_port, 80
    host, port = host_port.rsplit(":", 1)
    return host, int(port)


if __name__ == "__main__":
    raise SystemExit(main())
