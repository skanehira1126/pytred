import pandas as pd
import pytest

from pytred.exceptions import InvalidFunctionCalledError
from pytred.helpers.md_tabulator import MarkdownTableTabulator


@pytest.mark.parametrize("mode", ["row", "column"])
def test_build_markdown(mode):
    table = MarkdownTableTabulator(mode)
    if mode == "row":
        table.add_rows({"a": 1, "b": 2})
        table.add_rows({"a": 3, "b": 4})
    else:
        table.add_columns(a=[1, 3])
        table.add_columns(b=[2, 4])

    expected = pd.DataFrame({"a": [1, 3], "b": [2, 4]})
    assert table.build() == expected.to_markdown(index=False)
    assert table.build(index=True) == expected.to_markdown(index=True)


def test_reject_invalid_mode():
    with pytest.raises(ValueError):
        MarkdownTableTabulator("invalid")


def test_reject_columns_in_row_mode():
    table = MarkdownTableTabulator("row")
    with pytest.raises(InvalidFunctionCalledError):
        table.add_columns(a=[1])


def test_reject_rows_in_column_mode():
    table = MarkdownTableTabulator("column")
    with pytest.raises(InvalidFunctionCalledError):
        table.add_rows({"a": 1})


def test_reject_unequal_column_lengths():
    table = MarkdownTableTabulator("column")
    table.add_columns(a=[1, 2])

    with pytest.raises(ValueError):
        table.add_columns(b=[3])


def test_reject_inconsistent_row_keys():
    table = MarkdownTableTabulator("row")
    table.add_rows({"a": 1, "b": 2})

    with pytest.raises(ValueError):
        table.add_rows({"a": 3})
