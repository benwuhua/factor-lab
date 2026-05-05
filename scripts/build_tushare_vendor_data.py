#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from _bootstrap import add_src_to_path, project_root, suppress_runtime_warnings

suppress_runtime_warnings()
add_src_to_path()

from qlib_factor_lab.data_update import load_env_file
from qlib_factor_lab.tushare_data import get_tushare_token, write_security_master_history_from_tushare


def build_parser() -> argparse.ArgumentParser:
    root = project_root()
    parser = argparse.ArgumentParser(description="Export Tushare PIT-compatible vendor data into factor-lab CSVs.")
    parser.add_argument("--project-root", default=str(root))
    parser.add_argument("--instruments", nargs="+", required=True, help="Qlib symbols, e.g. SH600000 SZ000001.")
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--as-of-date", default="")
    parser.add_argument("--research-universe", default="")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--output", default="data/vendor/security_master_history_tushare.csv")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    root = Path(args.project_root).expanduser().resolve()
    env = load_env_file(root / args.env_file)
    token = get_tushare_token(env=env)
    output = write_security_master_history_from_tushare(
        root / args.output,
        instruments=args.instruments,
        start_date=args.start_date,
        as_of_date=args.as_of_date or args.end_date,
        research_universe=args.research_universe,
        token=token,
        delay=args.delay,
    )
    print(f"security_master_history: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
