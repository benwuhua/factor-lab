from __future__ import annotations

import sys
import warnings
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def add_src_to_path() -> None:
    src = project_root() / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


def suppress_runtime_warnings() -> None:
    warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL.*")
    try:
        from urllib3.exceptions import NotOpenSSLWarning
    except Exception:
        return
    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
