from pathlib import Path
import runpy

import pytest


ABSURDCTL_PATH = Path(__file__).resolve().parents[1] / "absurdctl"
MODULE = runpy.run_path(str(ABSURDCTL_PATH))
validate_queue_name = MODULE["validate_queue_name"]


@pytest.mark.parametrize(
    "queue_name",
    [
        "default",
        "1jobs",
        "queue_1",
        "queue-1",
        "9",
    ],
)
def test_validate_queue_name_accepts_supported_names(queue_name):
    assert validate_queue_name(queue_name) == queue_name


@pytest.mark.parametrize(
    "queue_name",
    [
        "",
        "-bad",
        "_bad",
        "bad space",
        "bad'quote",
    ],
)
def test_validate_queue_name_rejects_unsafe_names(queue_name):
    with pytest.raises(SystemExit):
        validate_queue_name(queue_name)
