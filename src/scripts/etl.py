from datetime import datetime

import dotenv
import polars as pl
from pipeline import get_gold_table
from validation import validate_df
from support import ingest_from_s3, sink_delta_to_s3
from schema import PA_SCHEMA, Output

dotenv.load_dotenv()


def main():
    base_path = "data/log_content/"
    start_date = "20220401"
    end_date = "20220430"
    app_names = [
        "CHANNEL",
        "KPLUS",
        "VOD",
        "FIMS",
        "BHD",
        "SPORT",
        "CHILD",
        "RELAX",
    ]

    column_names = [
        "TVDuration",
        "TVDuration",
        "MovieDuration",
        "MovieDuration",
        "MovieDuration",
        "SportDuration",
        "ChildDuration",
        "RelaxDuration",
    ]
    write_options = {"engine": "rust"}
    sources = ingest_from_s3(base_path, PA_SCHEMA).filter(
        pl.col("Date").is_between(datetime(2022, 4, 1), datetime(2022, 4, 2))
    )
    tables = get_gold_table(sources, app_names=app_names, column_names=column_names)
    validate_df(tables, Output)
    sink_delta_to_s3(
        tables,
        target="s3a://data/results",
        mode="overwrite",
        delta_write_options=write_options,
    )


if __name__ == "__main__":
    main()
