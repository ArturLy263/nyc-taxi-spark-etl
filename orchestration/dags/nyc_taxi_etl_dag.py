"""Airflow DAG that runs the NYC taxi pipeline: check input -> run ETL -> validate output.

The logic lives in orchestration/tasks.py; this file only wires it together.
The repository root must be importable (PYTHONPATH), see orchestration/README.md.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task

from orchestration import tasks


@dag(
    dag_id="nyc_taxi_etl",
    schedule=None,  # triggered manually; the logical date names the output folder
    start_date=datetime(2019, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["nyc-taxi", "spark", "demo"],
)
def nyc_taxi_etl():
    @task
    def check_input() -> dict:
        return tasks.check_input(tasks.config()["input_path"])

    @task
    def run_etl(checked: dict) -> dict:
        run_date = get_current_context()["ds"]
        output_dir = tasks.output_dir_for(tasks.config()["output_base"], run_date)
        return tasks.run_etl(checked["input_path"], output_dir)

    @task(retries=0)  # a failed check is a data problem, not a transient error
    def validate_output(result: dict) -> dict:
        return tasks.validate_output(result["output_dir"])

    validate_output(run_etl(check_input()))


nyc_taxi_etl()
