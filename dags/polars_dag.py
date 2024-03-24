import os

import dotenv
from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

dotenv.load_dotenv()
BASE_PATH = "data/log_content/"


@dag(
    schedule="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["polars pipeline"],
)
def polars_dag():
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    etl = DockerOperator(
        task_id="etl_with_polars",
        image="polarspipeline:latest",
        container_name="etl_polars",
        auto_remove=True,
        do_xcom_push=False,
        docker_url="unix://var/run/docker.sock",
        network_mode="polars-pipeline_7d91b8_airflow",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/Users/sonle/Documents/GitHub/PolarsPipeline/src",
                target="/opt/src",
                type="bind",
            )
        ],
        working_dir="/opt/src",
        command="python3 scripts/etl.py",
    )
    # clickhouse operator to ingest delta from s3
    ch_insert = ClickHouseOperator(
        task_id="ingest_to_clickhouse",
        database="default",
        sql=(
            """
        create database if not exists logcontent;
        """,
            f"""
        create table logcontent.results 
            engine = DeltaLake('http://miniostorage:9000/data/results', 
                {os.getenv("AWS_ACCESS_KEY_ID")}, 
                {os.getenv("AWS_SECRET_ACCESS_KEY")});
        """,
        ),
        query_id="ingest_to_clickhouse",
        clickhouse_conn_id="clickhouse_default",
    )

    begin >> etl >> ch_insert >> end


polars_dag()
