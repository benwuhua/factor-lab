#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.tushare_data import format_permission_probe_rows, get_tushare_token, probe_tushare_permissions


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Tushare permissions required by factor-lab without printing the token.")
    parser.add_argument("--output", default=None, help="Optional markdown report path.")
    args = parser.parse_args()

    token = get_tushare_token()
    rows = probe_tushare_permissions(token=token)
    report = format_permission_probe_rows(rows, token=token)
    print(report, end="")
    if args.output:
        output = Path(args.output)
        if not output.is_absolute():
            output = project_root() / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8")
    return 0 if rows and all(row.get("status") == "ok" for row in rows) else 1


if __name__ == "__main__":
    raise SystemExit(main())
