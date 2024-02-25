# %%
import polars as pl


def get_log_json(paths: str | list[str]) -> pl.LazyFrame:
    """
    Function to ingest the logging json data, get the filename and add a "Date" column
    Args:
        paths (str | list[str]): a list of path to data, this path must be in glob pattern

    Returns:
        pl.LazyFame
    """
    # Define schema of logging data
    schema = {
        "_index" : pl.String,
        "_type"  : pl.String,
        "_id"    : pl.String,
        "_score" : pl.Int64,
        "_source": pl.Struct(
            [
                pl.Field("Contract", pl.String),
                pl.Field("Mac", pl.String),
                pl.Field("TotalDuration", pl.Int64),
                pl.Field("AppName", pl.String),
            ]
        ),
    }

    def _scan_log(path, schema)  -> pl.LazyFrame:
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


def get_rfm_table(sources: pl.LazyFrame, reported_date: str, **options) -> pl.LazyFrame:
    """
    Function to get the recency, frequency, and monetary value of the data
    Args:
        sources (pl.LazyFrame): the source data
        reported_date (str): the date when the data is reported, must be in format "%Y %m %d"
        options: the options to calculate the rfm value

    Returns:
        pl.LazyFrame
    """
    total_date = options.get("total_date", 30)
    temp_tbl = sources.group_by("Contract").agg(pl.max("Date").alias("LastDate"))

    # recency_window = pl.Expr.p`
    # Get the recency, frequency, and monetary value
    rfm = (
        sources
        .groupby("Mac")
        .agg(
            pl.col("Date").max().alias("LastDate"),
            pl.count(pl.col("Date")).alias("Frequency"),
            pl.sum(pl.col("TotalDuration")).alias("Monetary"),
        )
    )
    # Calculate the recency, frequency, and monetary value
    rfm: pl.LazyFrame = rfm.with_columns(
        pl.col("LastDate").date_diff(reported_date, pl.DateUnits.Day).alias("Recency")
    )

    pl.all_horizontal()
    return rfm


def main() -> None:
    pass
