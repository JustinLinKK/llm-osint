from __future__ import annotations

import importlib.util
from pathlib import Path


def test_run_pipeline_module_imports_without_optional_deps() -> None:
    module_path = Path(__file__).resolve().parents[1] / "src" / "run_pipeline_test.py"
    spec = importlib.util.spec_from_file_location("run_pipeline_test", module_path)
    assert spec is not None and spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert callable(module.main)
    assert callable(module._get_dsn)
