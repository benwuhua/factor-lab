#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from _bootstrap import project_root


def main() -> int:
    root = project_root()
    app_path = root / "app/streamlit_app.py"
    try:
        import streamlit  # noqa: F401
    except ModuleNotFoundError:
        print("Streamlit is not installed. Run: .venv/bin/pip install -r requirements.txt", file=sys.stderr)
        return 1
    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.address",
        "127.0.0.1",
    ]
    return subprocess.call(command, cwd=root)


if __name__ == "__main__":
    raise SystemExit(main())
