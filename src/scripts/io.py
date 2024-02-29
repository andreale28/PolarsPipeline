import os
from typing import List, Mapping

import polars as pl
import pyarrow as pa
from pyarrow import fs
from pyarrow.dataset import dataset, write_dataset
import dotenv

dotenv.load_dotenv()


def ingest_from_s3(
    paths: List[str],
    schema: pa.Schema,
    base_path: str = None,
) -> pl.LazyFrame:
    """
    Ingests data from specified paths using PyArrow and returns a Polars LazyFrame.

    Args:
        paths (List[str]): List of paths to ingest data from.
        schema (pa.Schema): The schema to use for the data.
        base_path (str, optional): The base path for the file system. Defaults to None.

    Returns:
        pl.LazyFrame: A Polars LazyFrame containing the ingested data.
    """
    cloudfs = fs.S3FileSystem(
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_REGION"),
        endpoint_override="http://127.0.0.1:9000",
    )

    s3_files = [
        entry.path
        for entry in cloudfs.get_file_info(fs.FileSelector(base_path, recursive=True))
    ]

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


def ingest_from_local(
    paths: str | list[str],
    schema: Mapping[str, pl.DataType],
) -> pl.LazyFrame:
    """
    Function to ingest the logging json data, get the filename and add a "Date" column
    Args:
        paths (str | list[str]): a list of path to data, this path must be in glob pattern
        schema (Mapping[str, pl.DataType]): schema of local log json data

    Returns:
        pl.LazyFame
    """

    # Define schema of logging data

    if isinstance(paths, str):
        paths = [paths]

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


def sink_to_s3(sources: pl.LazyFrame, **options):
    s3fs = fs.S3FileSystem(
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_REGION"),
        endpoint_override="http://127.0.0.1:9000",
    )
    return write_dataset(
        sources.collect(streaming=True).to_arrow(),
        "s3://data/log_content.parquet/",
        filesystem=s3fs,
        format="parquet",
    )
