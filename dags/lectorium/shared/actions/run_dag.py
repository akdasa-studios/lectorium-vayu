# from datetime import datetime

# from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def run_dag(
    task_id: str,
    trigger_dag_id: str,
    dag_run_params: dict,
    wait_for_completion: bool = True,
    reset_dag_run: bool = False,
    # dag_run_id: str,
    **kwargs
):
    # current_datetime_string = datetime.now().strftime("%Y%m%d%H%M%S")
    # dag_run_id = f"{track_id}_process_audio_{current_datetime_string}"
    trigger = TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=trigger_dag_id,
        conf=dag_run_params,
        reset_dag_run=reset_dag_run,
        wait_for_completion=wait_for_completion,
        # trigger_run_id=dag_run_id,
    )
    trigger.execute(context=kwargs)
