import pytest

from pytred.helpers.notebooks import get_current_env
from pytred.helpers.notebooks import init_mermaid


def test_detect_interpreter():
    assert get_current_env() == "Interpreter"


def test_mermaid_requires_notebook():
    with pytest.raises(RuntimeError):
        init_mermaid()
