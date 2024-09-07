import pytest

from pytred.helpers.notebooks import get_current_env
from pytred.helpers.notebooks import init_mermaid


def test__get_current_env_on_pytest():
    assert get_current_env() == "Interpreter"


def test__raise_RuntimeError():
    with pytest.raises(RuntimeError):
        init_mermaid()
