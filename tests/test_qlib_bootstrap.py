import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from qlib_factor_lab.config import ProjectConfig
from qlib_factor_lab.qlib_bootstrap import init_qlib


class QlibBootstrapTests(unittest.TestCase):
    def test_init_qlib_uses_threading_backend_on_macos_to_avoid_stdin_spawn_failures(self):
        fake_qlib = types.ModuleType("qlib")
        fake_qlib.init = Mock()
        fake_constant = types.ModuleType("qlib.constant")
        fake_constant.REG_CN = "cn"
        fake_constant.REG_US = "us"

        with patch.dict(sys.modules, {"qlib": fake_qlib, "qlib.constant": fake_constant}):
            with patch("platform.system", return_value="Darwin"):
                with patch.dict(os.environ, {}, clear=True):
                    init_qlib(ProjectConfig(provider_uri=Path("/tmp/qlib"), market="csi500"))

        fake_qlib.init.assert_called_once_with(
            provider_uri="/tmp/qlib",
            region="cn",
            joblib_backend="threading",
            kernels=1,
        )

    def test_init_qlib_allows_runtime_backend_override(self):
        fake_qlib = types.ModuleType("qlib")
        fake_qlib.init = Mock()
        fake_constant = types.ModuleType("qlib.constant")
        fake_constant.REG_CN = "cn"
        fake_constant.REG_US = "us"

        env = {
            "FACTOR_LAB_QLIB_JOBLIB_BACKEND": "multiprocessing",
            "FACTOR_LAB_QLIB_KERNELS": "4",
        }
        with patch.dict(sys.modules, {"qlib": fake_qlib, "qlib.constant": fake_constant}):
            with patch("platform.system", return_value="Darwin"):
                with patch.dict(os.environ, env, clear=True):
                    init_qlib(ProjectConfig(provider_uri=Path("/tmp/qlib"), market="csi500"))

        fake_qlib.init.assert_called_once_with(
            provider_uri="/tmp/qlib",
            region="cn",
            joblib_backend="multiprocessing",
            kernels=4,
        )


if __name__ == "__main__":
    unittest.main()
