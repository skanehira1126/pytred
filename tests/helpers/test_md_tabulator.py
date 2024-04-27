from __future__ import annotations

import pandas as pd
import pytest

from pytred.exceptions import InvalidFunctionCalledError
from pytred.helpers.md_tabulator import MarkdownTableTabulator


@pytest.fixture
def sample_df():
    df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [1, 4, 9],
            "c": [1, 8, 27],
        }
    )
    return df


def test__column_mode(sample_df):

    md_tabulator = MarkdownTableTabulator(mode="column")

    for col in ["a", "b", "c"]:
        md_tabulator.add_columns(**{col: sample_df[col].values})

    assert sample_df.to_markdown() == md_tabulator.build(index=True)


def test__row_mode(sample_df):

    md_tabulator = MarkdownTableTabulator(mode="row")

    for _, row in sample_df.iterrows():
        md_tabulator.add_rows(
            {
                "a": row.a,
                "b": row.b,
                "c": row.c,
            }
        )
    assert sample_df.to_markdown() == md_tabulator.build(index=True)


def test__raise_ValueError_when_invalid_mode():
    """
    input invalid value when initialize
    """

    with pytest.raises(ValueError):
        _ = MarkdownTableTabulator(mode="hoge")


def test__raise_InvalidFunctionCalledError():

    with pytest.raises(InvalidFunctionCalledError):
        md_tabulator = MarkdownTableTabulator(mode="row")
        md_tabulator.add_columns(hoge=[1, 2, 3])

    with pytest.raises(InvalidFunctionCalledError):
        md_tabulator = MarkdownTableTabulator(mode="column")
        md_tabulator.add_rows({"a": 1, "b": 2})


def test__add_columns_raise_ValueError_when_length_mismatch():
    md_tabulator = MarkdownTableTabulator(mode="column")

    md_tabulator.add_columns(
        hoge=[1, 2, 3],
    )

    with pytest.raises(ValueError):
        md_tabulator.add_columns(
            fuga=[1, 2],
        )


def test__add_rows_raise_ValueError_when_keys_mismatch():
    md_tabulator = MarkdownTableTabulator(mode="row")

    md_tabulator.add_rows({"a": 1, "b": 2, "c": 3})

    with pytest.raises(ValueError):
        md_tabulator.add_rows({"a": 1})
