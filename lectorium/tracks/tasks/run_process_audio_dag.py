from datetime import datetime

from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# @task(
#     task_display_name="📦 Process Audio")
def run_process_audio_dag(
    track_id: str,
    path_source: str,
    path_original_dest: str,
    path_processed_dest: str,
    **kwargs
):
    current_datetime_string = datetime.now().strftime("%Y%m%d%H%M%S")
    dag_run_id = f"{track_id}_process_audio_{current_datetime_string}"
    trigger = TriggerDagRunOperator(
        task_id=f"trigger_target_dag",
        trigger_dag_id="dag_process_audio",
        trigger_run_id=dag_run_id,
        conf={
            "track_id": track_id,
            "path_source": path_source,
            "path_original_dest": path_original_dest,
            "path_processed_dest": path_processed_dest,
        },
        reset_dag_run=False,
        wait_for_completion=True,
    )
    trigger.execute(context=kwargs)
