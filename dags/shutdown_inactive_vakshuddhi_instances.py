from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils.session import create_session

import services as services
import lectorium as lectorium
import services.vastai as vastai


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="🔉 Vakshuddhi: Shutdown Inactive",
    description="Shuts down inactive Vakshuddhi instances",
    start_date=datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    tags=["lectorium", "audio"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
    },
    render_template_as_native_obj=True)
def shutdown_inactive_vakshuddhi_instances():

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    vastai_api_key = (
        Variable.get(lectorium.config.VASTAI_ACCESS_KEY)
    )


    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    # -------------------------- Active VastAI Instances ------------------------- #

    vastai_instances = (
        vastai.get_instances(vastai_api_key)
    )

    vastai_active_instances = (
        vastai.get_active_instances(vastai_instances, "vakshuddhi")
    )

    vastai_instances >> vastai_active_instances

    # ---------------------------- Get Active DAG Runs --------------------------- #

    @task(
        task_display_name="🔍 Active DAG Runs")
    def count_active_dag_runs(dag_id):
        count = 0
        states = [State.RUNNING, State.QUEUED]
        for state in states:
            active_dag_runs = DagRun.find(
                dag_id=dag_id, state=state)
            count += len(active_dag_runs)
        return count

    @task(
        task_display_name="🔍 Last DAG Run")
    def last_completed_dag_run(dag_id):
        with create_session() as session:
            last_dag_run = (
                session.query(DagRun)
                    .filter(DagRun.dag_id == dag_id)
                    .order_by(DagRun.execution_date.desc())
                    .first()
            )
        return last_dag_run.execution_date if last_dag_run else None

    # ------------------------------ Should Shutdown ----------------------------- #

    @task.branch(
        task_display_name="🤔 Should Shutdown Instances")
    def should_shutdown_instances(
        instances: list[vastai.Instance],
        active_dag_runs: int,
        last_dag_run: datetime,
    ):
        if not instances:
            print("No running vakshuddhi instance found")
            return None # No running vakshuddhi instance found
        if active_dag_runs:
            print("Active dag runs found")
            return None # No active dag runs found
        if datetime.now().replace(tzinfo=None) - last_dag_run.replace(tzinfo=None) < timedelta(minutes=10):
            print("Last dag run was less than 10 minutes ago")
            return None # Last dag run was less than 10 minutes ago

        return "shutdown_instance"

    active_dag_runs_count = (
        count_active_dag_runs("process_audio")
    )

    last_dag_run_date = (
        last_completed_dag_run("process_audio")
    )

    should_shutdown = (
        should_shutdown_instances(
            vastai_active_instances,
            active_dag_runs_count,
            last_dag_run_date)
    )

    shutdown_instances = (
        vastai.shutdown_instance
            .partial(vast_api_key=vastai_api_key)
            .expand(instance=vastai_active_instances)
    )

    # ----------------------------------- Flow ----------------------------------- #

    [
        active_dag_runs_count,
        last_dag_run_date,
        vastai_active_instances,
    ] >>  should_shutdown >> shutdown_instances


shutdown_inactive_vakshuddhi_instances()
