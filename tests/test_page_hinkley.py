import ast
import math
from datetime import datetime
from pathlib import Path
from typing import Dict


def _load_page_hinkley_class():
    source_path = Path("src/ivtool/detectors/page_hinkley.py")
    tree = ast.parse(source_path.read_text())
    class_node = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Page_Hinkley"
    )
    module = ast.Module(body=[class_node], type_ignores=[])
    namespace = {"Dict": Dict, "math": math}
    exec(compile(module, str(source_path), "exec"), namespace)
    return namespace["Page_Hinkley"]


def test_page_hinkley_high_regime_alarm():
    ph_cls = _load_page_hinkley_class()
    detector = ph_cls()
    detector.get_f = lambda _x: (1.0, math.exp(300.0))

    alarm = detector.update(0.2, datetime(2024, 1, 1, 0, 1))

    assert alarm is True
    assert detector.high_indices == [0]
    assert detector.low_indices == []
    assert detector.g_pos == 0.0
    assert detector.g_neg == 0.0


def test_page_hinkley_low_regime_alarm():
    ph_cls = _load_page_hinkley_class()
    detector = ph_cls()
    detector.get_f = lambda _x: (math.exp(300.0), 1.0)

    alarm = detector.update(0.2, datetime(2024, 1, 1, 0, 1))

    assert alarm is False
    assert detector.low_indices == [0]
    assert detector.high_indices == []


def test_page_hinkley_returns_none_without_alarm():
    ph_cls = _load_page_hinkley_class()
    detector = ph_cls()
    detector.get_f = lambda _x: (1.0, math.exp(1.0))

    alarm = detector.update(0.2, datetime(2024, 1, 1, 0, 1))

    assert alarm is None
    assert detector.high_indices == []
    assert detector.low_indices == []
