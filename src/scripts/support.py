import os
from typing import List, Mapping, Any

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

    for path in s3_files:
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
        paths (str | list[str]): a list of path to data, this path should be in glob pattern
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


def sink_to_s3(
    sources: pl.LazyFrame, path: str, compression: str = "zstd", **options
) -> None:
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


def sink_delta_to_s3(
    tables: pl.LazyFrame,
    target: str,
    mode: str = "append",
) -> None:
    """
    Sink the proprietary table to delta lake in s3.
    Caution: Only the dimensional table not the sources' tables
    Args:
        tables (pl.LazyFrame): a LazyFrame
        target (str): the path to store in s3 delta lake
        mode (str): mode to sink delta lake (append, overwite, error)

    Returns:

    """
    tbl = tables.collect(streaming=True)

    return tbl.write_delta(
        target=target,
        mode=mode,
        storage_options={
            "AWS_REGION": os.getenv("AWS_REGION"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        },
    )


def type2_scd_upsert_pl(
    sources_df: pl.LazyFrame,
    updates_df: pl.LazyFrame,
    primary_key: str,
    target: str,
    attr_cols: List[str],
    is_current_col: str = "is_current",
    effective_time_col: str = "effective_time",
    end_time_col: str = "end_time",
) -> dict[str, Any]:
    """
    Perform a Type 2 Slowly Changing Dimension (SCD) upsert operation using Polars LazyFrame/DataFrame and
    write to Delta Lake using pl.write_delta().
    Note that, the datatypes of **effective_time_col** and **end_time_col** should be in **datetime**

    Args:
        sources_df (pl.LazyFrame): The source or target Polars LazyFrame scanned from DeltaLake using pl.scan_delta().
        updates_df (pl.LazyFrame): The Polars LazyFrame representing the updates data.
        primary_key (str): The name of the primary key column.
        target (str): The name of the target table to write the upserted records.
        attr_cols (List[str]): A list of attribute column names.
        is_current_col (str, optional): The name of the column indicating if a record is current. Defaults to
        "is_current".
        effective_time_col (str, optional): The name of the column indicating the effective time of a record.
        Defaults to "effective_time".
        end_time_col (str, optional): The name of the column indicating the end time of a record. Defaults to
        "end_time".

    Returns:
        dict[str, Any]: A dictionary representing the result of the upsert operation.
    """
    # validate updates delta tables
    base_cols = sources_df.columns

    required_base_cols = (
        [primary_key] + attr_cols + [is_current_col, effective_time_col, end_time_col]
    )

    if sorted(base_cols) != sorted(required_base_cols):
        raise ValueError(
            f"The base columns, {base_cols!r}, has to be the same as the required columns: {required_base_cols!r}"
        )
    # validate updated polars lazyframe
    update_cols = updates_df.columns

    required_update_cols = [primary_key] + attr_cols + [effective_time_col]

    if sorted(update_cols) != sorted(required_update_cols):
        raise ValueError(
            f"The updates columns, {update_cols!r}, has to be the same as the required columns:"
            f" {required_update_cols!r}"
        )
    if not isinstance(sources_df.schema.get(effective_time_col), pl.Datetime):
        raise TypeError(
            f"Datatypes of {effective_time_col} should be in Datetime, got {sources_df.schema.get(effective_time_col)}"
        )
    if not isinstance(sources_df.schema.get(end_time_col), pl.Datetime):
        raise TypeError(
            f"Datatypes of {end_time_col} should be in Datetime, got {sources_df.schema.get(end_time_col)}"
        )
    if not isinstance(updates_df.schema.get(effective_time_col), pl.Datetime):
        raise TypeError(
            f"Datatypes of {effective_time_col} should be in Datetime, got {updates_df.schema.get(effective_time_col)}"
        )

    new_records = updates_df.join(sources_df, on=primary_key, how="anti").with_columns(
        pl.lit(True).alias(is_current_col),
        pl.lit(None, pl.Datetime).alias(end_time_col),
    )
    updates_conds = [
        (pl.col(f"{attr}") != pl.col(f"{attr}_right")) for attr in attr_cols
    ]
    updates_records = sources_df.join(updates_df, on=primary_key, how="inner").filter(
        pl.any_horizontal(
            (pl.col(is_current_col) == True),
            *updates_conds,
        )
    )
    open_exprs = [(pl.col(f"{attr}_right").alias(f"{attr}")) for attr in attr_cols]
    open_records = updates_records.select(
        pl.col(primary_key),
        *open_exprs,
        pl.lit(True).alias(is_current_col),
        pl.col(f"{effective_time_col}_right").alias(f"{effective_time_col}"),
        pl.lit(None, pl.Datetime).alias(end_time_col),
    )

    close_exprs = [(pl.col(f"{attr}").alias(f"{attr}")) for attr in attr_cols]
    close_records = updates_records.select(
        pl.col(primary_key),
        *close_exprs,
        pl.lit(False).alias(is_current_col),
        pl.col(effective_time_col),
        pl.col(f"{effective_time_col}_right").alias(end_time_col),
    )

    # merging
    upsert_records = pl.concat(
        [
            new_records,
            open_records,
            close_records,
        ],
        how="align",
    )

    return (
        upsert_records.collect(streaming=True)
        .write_delta(
            target=target,
            mode="merge",
            delta_merge_options={
                "predicate": f"source.{primary_key} = target.{primary_key}",
                "source_alias": "source",
                "target_alias": "target",
            },
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
