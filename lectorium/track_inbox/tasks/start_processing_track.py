from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@task(task_display_name="Track Inbox: Start Processing Track")
def start_processing_track(track_id: str, **kwargs):
    trigger = TriggerDagRunOperator(
        task_id=f"trigger_target_dag",
        trigger_dag_id="tracks_process",
        conf={"track_id": track_id},
        reset_dag_run=False,
        wait_for_completion=False,
    )
    trigger.execute(context=kwargs)
