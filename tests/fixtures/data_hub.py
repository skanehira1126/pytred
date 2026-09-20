import polars as pl

from pytred import DataHub
from pytred.decorators import polars_table


class BranchingHub(DataHub):
    """Combine two features derived from one input."""

    @polars_table(0)
    def doubled(self, source):
        """Double the input value."""
        return source.select("id", doubled=pl.col("value") * 2)

    @polars_table(0)
    def offset(self, source):
        return source.select("id", offset=pl.col("value") + 1)

    @polars_table(1, "id", join="left")
    def combined(self, doubled, offset):
        """Join both features.

        Add them to produce the score.
        """
        return doubled.join(offset, on="id").select(
            "id", score=pl.col("doubled") + pl.col("offset")
        )


class OptionalHub(DataHub):
    @polars_table(0, is_optional=True)
    def prepared(self, source):
        return source

    @polars_table(1, "id", join="left", is_optional=True)
    def result(self, prepared):
        return prepared
