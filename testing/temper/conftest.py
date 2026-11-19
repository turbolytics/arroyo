import os

import pytest

import harness


@pytest.fixture(scope="session", autouse=True)
def arroyo():
    """Starts a fresh Arroyo in the control-plane mode the Temper level names."""
    level = os.environ.get("TEMPER_LEVEL", "controller")
    harness.start_arroyo(harness.MODES[level])
    yield level
