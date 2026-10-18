"""A branching pipeline with two sequential joins for the interactive viewer."""

import polars as pl

from pytred import DataHub
from pytred.decorators import polars_table


class DemoHub(DataHub):
    @polars_table(0)
    def doubled(self, source):
        """Double each source value."""
        return source.select("id", doubled=pl.col("value") * 2)

    @polars_table(0)
    def offset(self, source):
        """Offset each source value by one."""
        return source.select("id", offset=pl.col("value") + 1)

    @polars_table(1, "id", join="left")
    def combined(self, doubled, offset):
        """Combine the two features before joining them to root_df."""
        return doubled.join(offset, on="id").select(
            "id", score=pl.col("doubled") + pl.col("offset")
        )

    @polars_table(2, "id", join="left")
    def attributes(self, lookup):
        """Attach the category after the feature join."""
        return lookup.select("id", "category")


hub = DemoHub(
    pl.DataFrame({"id": [1, 2, 3]}),
    source=pl.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]}),
    lookup=pl.DataFrame({"id": [1, 2, 3], "category": ["A", "B", "A"]}),
)
