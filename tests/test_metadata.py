from typing import Literal

from pydantic import ValidationError
import pytest

from pytred.metadata import ColumnMetadata
from pytred.metadata import TableMetadata


def test_create_metadata_with_minimal_arguments():
    column = ColumnMetadata(name="id")
    table = TableMetadata()

    assert column.name == "id"
    assert not column.nullable and not column.is_categorical
    assert column.categories == ()
    assert column.null_meaning is None
    assert column.updated_on is None
    assert table.roles == ()
    assert table.primary_key == ()
    assert table.columns == ()
    assert table.updated_on is None


def test_preserve_unique_columns_and_composite_key():
    columns = (ColumnMetadata(name="player_id"), ColumnMetadata(name="reference_date"))

    table = TableMetadata(primary_key=("player_id", "reference_date"), columns=columns)

    assert table.primary_key == ("player_id", "reference_date")
    assert table.columns == columns


@pytest.mark.parametrize(
    "model, fields, invalid_field",
    [
        (ColumnMetadata, {"name": "id", "nullabel": True}, "nullabel"),
        (TableMetadata, {"primary_keys": ("id",)}, "primary_keys"),
        (ColumnMetadata, {"name": "id", "nullable": "false"}, "nullable"),
        (TableMetadata, {"primary_key": ["id"]}, "primary_key"),
    ],
    ids=["unknown-column-field", "unknown-table-field", "coerced-flag", "coerced-key"],
)
def test_reject_metadata_that_would_lose_or_coerce_a_declaration(model, fields, invalid_field):
    with pytest.raises(ValidationError) as error:
        model(**fields)
    assert invalid_field in [issue["loc"][0] for issue in error.value.errors()]


def test_reject_duplicate_column_names():
    """Report duplicate names as a columns validation error even when descriptions differ."""
    columns = (
        ColumnMetadata(name="id", description="First definition"),
        ColumnMetadata(name="value"),
        ColumnMetadata(name="id", description="Another definition"),
    )

    with pytest.raises(ValidationError) as exc_info:
        TableMetadata(columns=columns)

    error = exc_info.value.errors()[0]
    assert error["loc"] == ("columns",)
    assert error["type"] == "value_error"
    assert "'id'" in error["msg"]


def test_validate_project_roles():
    """Accept roles in the project's Literal type and reject values outside it."""
    project_metadata = TableMetadata[Literal["feature", "master"]]

    table = project_metadata(roles=("feature", "master"))

    assert table.roles == ("feature", "master")
    with pytest.raises(ValidationError) as exc_info:
        project_metadata(roles=("feature", "unknown"))

    error = exc_info.value.errors()[0]
    assert error["loc"] == ("roles", 1)
    assert error["type"] == "literal_error"


@pytest.mark.parametrize(
    "metadata",
    [ColumnMetadata(name="id"), TableMetadata()],
    ids=["column", "table"],
)
def test_reject_metadata_assignment(metadata):
    """Reject reassignment on column and table metadata while preserving the original value."""
    with pytest.raises(ValidationError) as exc_info:
        metadata.description = "Changed"

    assert exc_info.value.errors()[0]["type"] == "frozen_instance"
    assert metadata.description is None
