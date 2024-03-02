# %%
from typing import List

import dotenv
import polars as pl

from io import sink_to_s3, ingest_from_s3
dotenv.load_dotenv()


def get_rfm_table(
    sources: pl.LazyFrame,
    reported_date: str = "20220501",
    total_date: int = 30,
) -> pl.LazyFrame:
    if not isinstance(sources, pl.LazyFrame):
        sources = sources.lazy()

    b: pl.LazyFrame = sources.group_by("Contract").agg(
        pl.col("Date").max().alias("LatestDate")
    )

    temp = sources.with_columns(
        pl.lit(reported_date).str.to_date("%Y %m %d").alias("ReportedDate")
    )

    rfm = (
        temp.filter(pl.col("Contract").str.len_chars() > 1)
        .join(b, on="Contract", how="left")
        .group_by("Contract")
        .agg(
            (pl.col("ReportedDate") - pl.col("LatestDate")).min().alias("Recency"),
            (pl.col("Date").n_unique().cast(pl.Float32) / pl.lit(total_date) * 100.0)
            .round(2)
            .alias("Frequency"),
            pl.col("TotalDuration").sum().alias("Monetary"),
        )
        .with_columns(
            pl.col("Recency")
            .qcut(3, labels=["1", "2", "3"], allow_duplicates=True)
            .alias("R"),
            pl.col("Frequency")
            .qcut(3, labels=["1", "2", "3"], allow_duplicates=True)
            .alias("F"),
            pl.col("Monetary")
            .qcut(3, labels=["1", "2", "3"], allow_duplicates=True)
            .alias("M"),
        )
    )

    return rfm


def get_pivot_data(
    sources: pl.LazyFrame,
    app_names: List[str],
    column_names: List[str],
) -> pl.LazyFrame:
    """
    Function to pivot data based on the provided sources, app_names, and column_names.

    Args:
        sources (pl.LazyFrame): The input lazy frame containing the data.
        app_names (List[str]): The list of application names.
        column_names (List[str]): The list of column names.

    Returns:
        pl.LazyFrame: The pivoted lazy frame based on the provided data.
    """
    if not isinstance(sources, pl.LazyFrame):
        sources = sources.lazy()

    if len(app_names) != len(column_names):
        raise ValueError("The lengths of app_names and column_names must be the same")

    mapping = dict(zip(app_names, column_names))
    pivot_df: pl.LazyFrame = (
        sources.select(
            pl.col("Contract"),
            pl.col("TotalDuration"),
            pl.col("AppName").replace(mapping, default="Unknown").alias("Type"),
        )
        .filter(
            (pl.col("Contract").str.len_chars() > 1)
            & (pl.col("Type") != "Unknown")
            & (pl.col("TotalDuration") > 0)
        )
        .group_by(["Contract"])
        .agg(
            [
                pl.when(pl.col("Type") == y)
                .then(pl.col("TotalDuration"))
                .sum()
                .alias(y)
                for y in set(column_names)
            ]
        )
        .sort(["Contract", "TVDuration"])
    )

    return pivot_df


def get_most_watch(sources: pl.LazyFrame) -> pl.LazyFrame:
    """
    Get the most watched item for each contract in the LazyFrame.

    Args:
        sources (pl.LazyFrame): The pivot lazyframe from the get_pivot_data function

    Returns:
        pl.LazyFrame: The LazyFrame with the most watched item for each contract.
    """
    if not isinstance(sources, pl.LazyFrame):
        sources = sources.lazy()
    columns = sources.columns[1:]
    watch_type = [item[:-8] for item in columns]

    return sources.with_columns(
        pl.concat_list(
            [
                pl.struct(pl.col(c).alias("l"), pl.lit(v).alias("k"))
                for c, v in zip(columns, watch_type)
            ]
        ).alias("temp")
    ).select(
        pl.col("Contract"),
        pl.col("temp")
        .list.sort(descending=True)
        .list.first()
        .struct.field("k")
        .alias("MostWatch"),
    )


def main() -> None:
    pass
