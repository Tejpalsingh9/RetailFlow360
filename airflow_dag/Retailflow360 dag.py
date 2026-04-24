from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta


# ── CONFIG ─────────────────────────────────────────────
TEAM = "tejpal, utkarsh, michael, vansh"
PROJECT = "retailflow360"

NOTEBOOK_ROOT = "/Workspace/Users/tejpallsingh009@gmail.com"
EMAIL = "t1116325@gmail.com"


# ── DEFAULT ARGS ───────────────────────────────────────
default_args = {
    "owner": TEAM,
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── TASK FACTORY ──────────────────────────────────────
def make_serverless_task(task_id, notebook_path, params=None, timeout=7200):
    if params is None:
        params = {}

    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id="databricks_default",
        execution_timeout=timedelta(seconds=timeout),
        json={
            "run_name": task_id,
            "tasks": [
                {
                    "task_key": task_id,
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": params
                    },
                    "environment_key": "Default",
                    "timeout_seconds": timeout
                }
            ],
            "environments": [
                {
                    "environment_key": "Default",
                    "spec": {
                        "client": "2"
                    }
                }
            ]
        }
    )


# ── EMAIL HTML ─────────────────────────────────────────
SUCCESS_HTML = """
<h2>✅ RetailFlow360 Pipeline SUCCESS</h2>
<p>Run date: {{ ds }}</p>
<p>DAG run ID: {{ run_id }}</p>
"""

FAILURE_HTML = """
<h2>❌ RetailFlow360 Pipeline FAILED</h2>
<p>Run date: {{ ds }}</p>
<p>DAG run ID: {{ run_id }}</p>
"""


# ── DAG ───────────────────────────────────────────────
with DAG(
    dag_id="retailflow360_pipeline",
    description="RetailFlow360 end-to-end pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["retailflow360", "databricks", "medallion"],
) as dag:


    # ── SETUP ─────────────────────────────────────────
    with TaskGroup("setup_group") as setup_group:

        t_config = make_serverless_task(
            task_id="run_config_setup",
            notebook_path=f"{NOTEBOOK_ROOT}/00_config_setup"
        )

        t_ddl = make_serverless_task(
            task_id="run_ddl_create_tables",
            notebook_path=f"{NOTEBOOK_ROOT}/00_ddl_create_tables"
        )

        t_config >> t_ddl


    # ── BRONZE ─────────────────────────────────────────
    with TaskGroup("bronze_group") as bronze_group:

        t_bronze = make_serverless_task(
            task_id="run_bronze_ingestion",
            notebook_path=f"{NOTEBOOK_ROOT}/01_bronze_ingestion retailflow360"
        )


    # ── SILVER ─────────────────────────────────────────
    with TaskGroup("silver_group") as silver_group:

        t_silver_clean = make_serverless_task(
            task_id="run_silver_clean",
            notebook_path=f"{NOTEBOOK_ROOT}/02_silver_clean retailflow360"
        )

        t_silver_dq = make_serverless_task(
            task_id="run_silver_dq_quarantine",
            notebook_path=f"{NOTEBOOK_ROOT}/02_silver_dq_quarantine"
        )

        t_silver_clean >> t_silver_dq


    # ── GOLD ───────────────────────────────────────────
    with TaskGroup("gold_group") as gold_group:

        t_gold = make_serverless_task(
            task_id="run_gold_starschema",
            notebook_path=f"{NOTEBOOK_ROOT}/03_gold_starschema"
        )


    # ── PLATINUM ───────────────────────────────────────
    with TaskGroup("platinum_group") as platinum_group:

        t_platinum = make_serverless_task(
            task_id="run_platinum_serving_layer",
            notebook_path=f"{NOTEBOOK_ROOT}/04_platinum_serving_layer"
        )


    # ── EMAIL TASKS ────────────────────────────────────
    email_success = EmailOperator(
        task_id="email_success",
        to=EMAIL,   # ✅ FIXED
        subject="✅ RetailFlow360 SUCCESS — {{ ds }}",
        html_content=SUCCESS_HTML,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    email_failure = EmailOperator(
        task_id="email_failure",
        to=EMAIL,   # ✅ FIXED
        subject="❌ RetailFlow360 FAILED — {{ ds }}",
        html_content=FAILURE_HTML,
        trigger_rule=TriggerRule.ONE_FAILED,
    )


    # ── FLOW ───────────────────────────────────────────
    setup_group >> bronze_group >> silver_group >> gold_group >> platinum_group

    platinum_group >> email_success
    platinum_group >> email_failure