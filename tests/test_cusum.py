import ast
from pathlib import Path
from typing import Dict


def _load_cusum_class():
    source_path = Path("src/ivtool/detectors/cusum.py")
    tree = ast.parse(source_path.read_text())
    class_node = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "CUSUM"
    )
    module = ast.Module(body=[class_node], type_ignores=[])
    namespace = {"Dict": Dict}
    exec(compile(module, str(source_path), "exec"), namespace)
    return namespace["CUSUM"]


def test_cusum_detects_upward_shift_and_resets_state():
    cusum_cls = _load_cusum_class()
    detector = cusum_cls(k=0.5, h=2.0, mu=0.0)

    alarms = [detector.update(x) for x in [0.4, 1.1, 1.1, 1.1, 1.1]]

    assert alarms == [False, False, False, False, True]
    assert detector.state() == {"gp": 0.0, "gn": 0.0, "t": 0.0}


def test_cusum_detects_downward_shift():
    cusum_cls = _load_cusum_class()
    detector = cusum_cls(k=0.5, h=2.0, mu=0.0)

    alarms = [detector.update(x) for x in [-0.4, -1.1, -1.1, -1.1, -1.1]]

    assert alarms == [False, False, False, False, True]
