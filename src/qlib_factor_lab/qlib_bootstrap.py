from __future__ import annotations

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
    qlib.init(provider_uri=provider_uri, region=region)
