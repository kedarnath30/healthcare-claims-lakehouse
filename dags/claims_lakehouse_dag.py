"""Airflow DAG: CMS claims lakehouse, download -> bronze -> silver -> gold -> SQL parity.

Each layer task runs the same CLI used locally (`python -m src.lakehouse.pipeline --layer <x> --strict`), so a
failed data-quality gate (exit code 1) stops the downstream layers. On Databricks, swap the BashOperators for
`DatabricksSubmitRunOperator` / `DatabricksNotebookOperator` pointing at notebooks/databricks/0x_*.py (example
config at the bottom, behind USE_DATABRICKS).

Set the Airflow Variable `claims_lakehouse_repo` to the checkout path (default /opt/airflow/repos/healthcare-claims-lakehouse).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Resolved at run time through Jinja (no metadata-DB query while the scheduler parses this file)
REPO = "{{ var.value.get('claims_lakehouse_repo', '/opt/airflow/repos/healthcare-claims-lakehouse') }}"
USE_DATABRICKS = os.environ.get("CLAIMS_LAKEHOUSE_ON_DATABRICKS", "false").lower() == "true"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="cms_claims_lakehouse",
    description="CMS DE-SynPUF claims: bronze -> silver -> gold medallion pipeline with DQ gates",
    start_date=datetime(2026, 10, 1),
    schedule="@monthly",          # claims files land monthly in a real payer feed
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["healthcare", "claims", "lakehouse", "spark"],
    doc_md=__doc__,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    if not USE_DATABRICKS:
        def layer_cmd(layer: str) -> str:
            return f"cd {REPO} && python -m src.lakehouse.pipeline --layer {layer} --strict"

        download = BashOperator(task_id="download_raw", bash_command=f"cd {REPO} && python scripts/download_data.py")
        bronze = BashOperator(task_id="bronze_ingest", bash_command=layer_cmd("bronze"))
        silver = BashOperator(task_id="silver_conform", bash_command=layer_cmd("silver"))
        gold = BashOperator(task_id="gold_marts", bash_command=layer_cmd("gold"))
        sql_parity = BashOperator(task_id="gold_sql_parity", bash_command=f"cd {REPO} && python -m src.run_gold_sql")
        figures = BashOperator(task_id="figures", bash_command=f"cd {REPO} && python -m src.make_figures")

        start >> download >> bronze >> silver >> gold >> [sql_parity, figures] >> end
    else:  # pragma: no cover - needs the Databricks provider and a workspace connection
        from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

        cluster = {"spark_version": "15.4.x-scala2.12", "node_type_id": "i3.xlarge", "num_workers": 2}
        tasks = []
        for i, nb in enumerate(["01_bronze_ingest", "02_silver_conform", "03_gold_marts", "04_gold_sql_parity"]):
            tasks.append(DatabricksSubmitRunOperator(
                task_id=nb,
                databricks_conn_id="databricks_default",
                new_cluster=cluster,
                notebook_task={"notebook_path": f"/Repos/data-eng/healthcare-claims-lakehouse/notebooks/databricks/{nb}"},
            ))
        start >> tasks[0] >> tasks[1] >> tasks[2] >> tasks[3] >> end
