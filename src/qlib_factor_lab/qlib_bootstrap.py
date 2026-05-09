from __future__ import annotations

import os
import platform
from pathlib import Path

from .config import ProjectConfig


def init_qlib(config: ProjectConfig) -> None:
    try:
        import qlib
        from qlib.constant import REG_CN, REG_US
    except ImportError as exc:
        raise RuntimeError("pyqlib is not installed. Run `python -m pip install -r requirements.txt`.") from exc

    region = REG_CN if config.region.lower() == "cn" else REG_US
    provider_uri = str(Path(config.provider_uri).expanduser())
    qlib.init(provider_uri=provider_uri, region=region, **_runtime_kwargs())


def _runtime_kwargs() -> dict[str, object]:
    backend = os.environ.get("FACTOR_LAB_QLIB_JOBLIB_BACKEND")
    kernels = os.environ.get("FACTOR_LAB_QLIB_KERNELS")
    if platform.system() == "Darwin":
        backend = backend or "threading"
        kernels = kernels or "1"

    kwargs: dict[str, object] = {}
    if backend:
        kwargs["joblib_backend"] = backend
    if kernels:
        kwargs["kernels"] = int(kernels)
    return kwargs
