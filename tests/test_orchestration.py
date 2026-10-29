"""Tests for the Airflow task logic (no Airflow needed) and, when Airflow is
installed, for the DAG's structure."""

import pytest

from orchestration import tasks


def test_config_reads_environment(monkeypatch):
    monkeypatch.setenv("NYC_TAXI_INPUT", "/data/in.parquet")
    monkeypatch.setenv("NYC_TAXI_OUTPUT_DIR", "/data/out")
    assert tasks.config() == {"input_path": "/data/in.parquet", "output_base": "/data/out"}


def test_default_input_is_the_sample(monkeypatch):
    monkeypatch.delenv("NYC_TAXI_INPUT", raising=False)
    assert tasks.config()["input_path"].endswith("examples/sample_trips.csv")


def test_output_dir_is_per_run_date():
    assert tasks.output_dir_for("/out", "2019-01-01") == "/out/run_date=2019-01-01"


def test_check_input_rejects_missing_and_empty_files(tmp_path):
    with pytest.raises(FileNotFoundError):
        tasks.check_input(str(tmp_path / "nope.csv"))
    empty = tmp_path / "empty.csv"
    empty.write_text("")
    with pytest.raises(ValueError, match="empty"):
        tasks.check_input(str(empty))


def test_run_and_validate_are_repeatable(spark, tmp_path):
    checked = tasks.check_input(tasks.config()["input_path"])
    out = tasks.output_dir_for(str(tmp_path), "2019-01-01")

    first = tasks.run_etl(checked["input_path"], out)
    second = tasks.run_etl(checked["input_path"], out)  # a retry or re-run overwrites
    assert first == second == {"output_dir": out, "trips": 12}

    assert tasks.validate_output(out) == {"trips": 12, "pickup_dates": ["2019-01-01", "2019-01-02"]}
    assert not spark.sparkContext._jsc.sc().isStopped()  # the test session was reused, not stopped


def test_validate_fails_on_missing_output(tmp_path):
    with pytest.raises(ValueError, match="Missing or incomplete outputs"):
        tasks.validate_output(str(tmp_path))


def test_dag_structure():
    pytest.importorskip("airflow")
    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder="orchestration/dags", include_examples=False)
    assert bag.import_errors == {}
    dag = bag.dags["nyc_taxi_etl"]  # get_dag() would query the metadata database
    assert [t.task_id for t in dag.topological_sort()] == ["check_input", "run_etl", "validate_output"]
    assert dag.get_task("run_etl").retries == 2
    assert dag.get_task("validate_output").retries == 0
