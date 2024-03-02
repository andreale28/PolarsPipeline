import os
from typing import List, Mapping

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
from pyarrow.dataset import dataset
import dotenv

dotenv.load_dotenv()


def ingest_from_s3(
    paths: List[str],
    schema: pa.Schema,
    base_path: str = None,
) -> pl.LazyFrame:
    """
    Ingests data from specified paths using PyArrow, create a columns with date getting from the filename,
    and returns a
    Polars
    LazyFrame.

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


def sink_to_s3(sources: pl.LazyFrame, path: str, compression: str = "zstd", **options):
    """
    Writes a Polars LazyFrame to an S3 bucket as a Parquet file.

    This function collects the data from the LazyFrame, converts it to an Arrow table, and then writes it to the
    specified S3 path.
    The AWS credentials and region are fetched from environment variables. The S3 endpoint is overridden to
    "http://127.0.0.1:9000".

    Args:
        sources (pl.LazyFrame): The LazyFrame to write to S3.
        path (str): The S3 path where the data should be written.
        compression (str): The compression is default to "zstd"
        **options: Additional options to pass to the pa.parquet.write_to_dataset or
        pa.parquet.write_table

    Returns:
        The result of the `write_dataset` function.

    Raises:
        Any exceptions raised by `write_dataset` will be propagated.
    """
    tbl = sources.collect(streaming=True).to_arrow()

    s3fs = fs.S3FileSystem(
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region=os.getenv("AWS_REGION"),
        endpoint_override="http://127.0.0.1:9000",
    )

    if options is None:
        options = {}

    options["compression"] = compression

    if options.get("partition_cols"):
        pq.write_to_dataset(
            table=tbl,
            root_path=path,
            filesystem=s3fs,
            **(options or {}),
        )
    else:
        pq.write_table(
            table=tbl,
            where=path,
            filesystem=s3fs,
            **(options or {}),
        )
