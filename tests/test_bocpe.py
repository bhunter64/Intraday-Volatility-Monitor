import importlib.util
import sys
from pathlib import Path

import pytest


def _load_bocpe_class():
    source_path = Path("src/ivtool/detectors/bocpe.py")
    spec = importlib.util.spec_from_file_location("bocpe_module", source_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module.BOCPE


@pytest.mark.parametrize(
    "kwargs",
    [
        {"hazard": 0.0},
        {"hazard": 1.0},
        {"threshold": 0.0},
        {"threshold": 1.0},
        {"prior_precision": 0.0},
        {"obs_var": 0.0},
        {"max_run_length": 0},
    ],
)
def test_bocpe_invalid_init_arguments_raise_value_error(kwargs):
    bocpe_cls = _load_bocpe_class()
    with pytest.raises(ValueError):
        bocpe_cls(**kwargs)


def test_bocpe_update_advances_state_without_triggering_on_stable_data():
    bocpe_cls = _load_bocpe_class()
    detector = bocpe_cls(hazard=0.05, threshold=0.9, prior_mean=0.0, prior_precision=1.0, obs_var=1.0)

    triggers = [detector.update(x) for x in [0.01, -0.02, 0.01, 0.0]]
    state = detector.state()

    assert triggers == [False, False, False, False]
    assert state["t"] == 4.0
    assert 0.0 <= state["cp_prob"] <= 1.0
    assert state["posterior_peak_prob"] <= 1.0


def test_bocpe_respects_max_run_length_truncation():
    bocpe_cls = _load_bocpe_class()
    detector = bocpe_cls(max_run_length=3, hazard=0.05, threshold=0.99)

    for value in [0.1, 0.0, -0.1, 0.2, -0.2, 0.1]:
        detector.update(value)

    assert len(detector._state.run_length_probs) == 4
    assert abs(sum(detector._state.run_length_probs) - 1.0) < 1e-12
