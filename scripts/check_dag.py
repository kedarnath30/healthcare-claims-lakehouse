"""Load dags/ into an Airflow DagBag and fail on import errors or a broken task graph."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

from airflow.models import DagBag  # noqa: E402

bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
if bag.import_errors:
    print("IMPORT ERRORS:", bag.import_errors)
    sys.exit(1)
dag = bag.dags.get("cms_claims_lakehouse")  # parsed DAGs only; no metadata DB needed
assert dag is not None, "cms_claims_lakehouse not found"
order = [t.task_id for t in dag.topological_sort()]
print("tasks in order:", order)
assert order.index("bronze_ingest") < order.index("silver_conform") < order.index("gold_marts")
print("DAG OK:", len(dag.tasks), "tasks")
