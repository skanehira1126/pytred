import datetime
from typing import Generic
from typing import TypeVar

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator


RoleT = TypeVar("RoleT", bound=str)


class ColumnMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    name: str = Field()
    description: str | None = Field(default=None)
    unit: str | None = Field(default=None, description="unit or scale of the column values")

    # Categorical variable
    is_categorical: bool = Field(default=False)
    categories: tuple[str | int, ...] = Field(
        default_factory=tuple,
        description="Possible category values; an empty tuple means unspecified",
    )

    # Null handling
    nullable: bool = Field(default=False)
    null_meaning: str | None = Field(default=None, description="Meaning of null values")

    updated_on: datetime.date | None = Field(
        default=None,
        description="Date the metadata was last updated",
    )


class TableMetadata(BaseModel, Generic[RoleT]):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    description: str | None = Field(default=None)
    roles: tuple[RoleT, ...] = Field(default_factory=tuple)
    grain: str | None = Field(default=None, description="What each row represents")
    primary_key: tuple[str, ...] = Field(default_factory=tuple)
    columns: tuple[ColumnMetadata, ...] = Field(default_factory=tuple)

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
