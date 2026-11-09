import datetime
from typing import Generic
from typing import TypeVar

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator


RoleT = TypeVar("RoleT", bound=str)


class ColumnMetadata(BaseModel):
    """Describe a column without validating its data.

    Only `name` is required. For example, `ColumnMetadata(name="amount", unit="JPY")`
    describes a monetary column.
    """

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    name: str = Field(description="Column name")
    description: str | None = Field(default=None, description="What the values represent")
    unit: str | None = Field(default=None, description="unit or scale of the column values")

    # Categorical variable
    is_categorical: bool = Field(
        default=False, description="Whether the column represents categories"
    )
    categories: tuple[str | int, ...] = Field(
        default_factory=tuple,
        description="Possible category values; an empty tuple means unspecified",
    )

    # Null handling
    nullable: bool = Field(default=False, description="Whether null values are expected")
    null_meaning: str | None = Field(default=None, description="Meaning of null values")

    updated_on: datetime.date | None = Field(
        default=None,
        description="Date the metadata was last updated",
    )


class TableMetadata(BaseModel, Generic[RoleT]):
    """Describe a table without validating its rows or enforcing its keys.

    All fields are optional. For example, `TableMetadata(primary_key=("id",))`
    describes a table whose key is `id`. Narrow role labels for static typing
    with `TableMetadata[ProjectRole]`, where `ProjectRole` is a string type.
    """

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    description: str | None = Field(default=None, description="What the table contains")
    roles: tuple[RoleT, ...] = Field(
        default_factory=tuple, description="Role labels for the table"
    )
    grain: str | None = Field(default=None, description="What each row represents")
    primary_key: tuple[str, ...] = Field(
        default_factory=tuple, description="Names of the columns that identify a row"
    )
    columns: tuple[ColumnMetadata, ...] = Field(
        default_factory=tuple, description="Column descriptions; names must be unique"
    )

    updated_on: datetime.date | None = Field(
        default=None,
        description="Date the metadata was last updated",
    )

    @field_validator("columns", mode="after")
    @classmethod
    def validate_unique_column_names(
        cls, columns: tuple[ColumnMetadata, ...]
    ) -> tuple[ColumnMetadata, ...]:
        seen: set[str] = set()

        for column in columns:
            if column.name in seen:
                raise ValueError(f"Duplicate column name: {column.name!r}")
            seen.add(column.name)

        return columns
