from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@task(task_display_name="Run 'Add to Inbox' DAG")
def run_tracks_add_to_inbox_dag(file_path: str, **kwargs):
    print("Running 'Add to Inbox' DAG: ", file_path)
    trigger = TriggerDagRunOperator(
        task_id=f"trigger_target_dag",
        trigger_dag_id="tracks_add_to_inbox",
        conf={"file_path": file_path},
        reset_dag_run=False,
        wait_for_completion=False,
    )
    trigger.execute(context=kwargs)
