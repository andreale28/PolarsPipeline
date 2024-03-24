import os
import dotenv
from datetime import datetime

from airflow.decorators import dag, task

from src.scripts.pipeline import get_gold_table
from src.scripts.support import sink_delta_to_s3, ingest_from_s3
from src.scripts.schema import PA_SCHEMA

dotenv.load_dotenv()


@dag(
    schedule="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["polars pipeline"],
)
def polars_dag():
    @task
    def sink_raw_to_s3():
        BASE_PATH = "data/log_content/"
        write_options = {
            "engine": "rust",
            "partition": "Date",
        }
        df = ingest_from_s3(BASE_PATH, schema=PA_SCHEMA)
        sink_delta_to_s3(
            df,
            target="s3://data/log_delta",
            mode="overwrite",
            delta_write_options=write_options,
        )

    sink_to_s3 = sink_raw_to_s3()

    @task
    def optimize_raw_tbls():
        from deltalake import DeltaTable

        table_uri = "s3://data/log_delta"
        dt = DeltaTable(
            table_uri=table_uri,
            storage_options={
                "AWS_REGION": os.getenv("AWS_REGION"),
                "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "AWS_ENDPOINT_URL": "http://miniostorage:9000",
                "AWS_ALLOW_HTTP": "true",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            },
        )
        print("Starting to optimize tables")
        dt.optimize.compact()
        dt.optimize.z_order(["Date", "Contract"])
        print("Finishing optimizing tables")

    optimize_tables = optimize_raw_tbls()

    @task
    def get_gold_tbls():
        import polars as pl

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
        df = pl.scan_delta(
            "s3://data/log_delta",
            storage_options={
                "AWS_REGION": os.getenv("AWS_REGION"),
                "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
                "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
                "AWS_ENDPOINT_URL": "http://miniostorage:9000",
                "AWS_ALLOW_HTTP": "true",
                "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            },
        ).filter(pl.col("Date").is_between(datetime(2022, 4, 1), datetime(2022, 4, 2)))

        tables = get_gold_table(df, app_names=app_names, column_names=column_names)

        sink_delta_to_s3(
            tables,
            target="s3://data/results_delta",
            mode="overwrite",
            delta_write_options=write_options,
        )

    get_gold_tbls = get_gold_tbls()

    sink_to_s3 >> optimize_tables >> get_gold_tbls


polars_dag()
