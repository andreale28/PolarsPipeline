# %%
import os
from typing import List

import dotenv
import polars as pl
import pyarrow as pa
from pyarrow import fs
from pyarrow.dataset import dataset


def get_log_json(paths: str | list[str]) -> pl.LazyFrame:
    """
    Function to ingest the logging json data, get the filename and add a "Date" column
    Args:
        paths (str | list[str]): a list of path to data, this path must be in glob pattern

    Returns:
        pl.LazyFame
    """
    # Define schema of logging data

    if isinstance(paths, str):
        paths = [paths]

    schema = {
        "_index": pl.String,
        "_type": pl.String,
        "_id": pl.String,
        "_score": pl.Int64,
        "_source": pl.Struct(
            [
                pl.Field("Contract", pl.String),
                pl.Field("Mac", pl.String),
                pl.Field("TotalDuration", pl.Int64),
                pl.Field("AppName", pl.String),
            ]
        ),
    }

    def _scan_log(path, schema) -> pl.LazyFrame:
        return (
            pl.scan_ndjson(path, schema=schema, low_memory=True)
            .with_columns(pl.Series("Date", [path]).str.extract(r"\d{8}", 0))
            .select(
                pl.col("Date").str.to_date("%Y %m %d"),
                pl.col("_index").alias("Index"),
                pl.col("_type").alias("Type"),
                pl.col("_id").alias("Id"),
                pl.col("_score").alias("Score"),
                pl.col("_source"),
            )
            .unnest("_source")
        )

    dfs = []
    # Function to scan and preprocess a single log data
    try:
        dfs = [_scan_log(path, schema) for path in paths]
    except Exception as e:
        print(f"Failed to read data from {paths}", {e})

    # Concatenate the processed lazyframes
    return pl.concat(dfs, how="vertical").lazy()


def get_rfm_table(
    sources: pl.LazyFrame, reported_date: str = "20220501", total_date: int = 30
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


# setup cloud filesystem access
def ingest_by_pyarrow(paths: List[str] = None):
    dotenv.load_dotenv()
    cloudfs = fs.S3FileSystem(
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_REGION"),
        endpoint_override="http://127.0.0.1:9000",
    )

    schema = pa.schema(
        [
            pa.field("_index", pa.string()),
            pa.field("_type", pa.string()),
            pa.field("_id", pa.string()),
            pa.field("_score", pa.int64()),
            pa.field(
                "_source",
                pa.struct(
                    [
                        pa.field("Contract", pa.string()),
                        pa.field("Mac", pa.string()),
                        pa.field("TotalDuration", pa.int64()),
                        pa.field("AppName", pa.string()),
                    ]
                ),
            ),
        ]
    )
    df = []

    for path in paths:
        ds = dataset(
            source=path,
            schema=schema,
            filesystem=cloudfs,
            format="json",
        )
        df.append(
            pl.scan_pyarrow_dataset(ds)
            .with_columns(pl.Series("Date", [path]).str.extract(r"\d{8}", 0))
            .select(
                pl.col("Date").str.to_date("%Y %m %d"),
                pl.col("_index").alias("Index"),
                pl.col("_type").alias("Type"),
                pl.col("_id").alias("Id"),
                pl.col("_score").alias("Score"),
                pl.col("_source"),
            )
            .unnest("_source")
        )

    return pl.concat(df)


def main() -> None:
    pass
