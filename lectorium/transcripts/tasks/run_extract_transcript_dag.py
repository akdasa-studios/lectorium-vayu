from datetime import datetime

from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@task(
    task_display_name="📝 Extract Transcript",
    map_index_template="{{ task.op_kwargs['language'] }}")
def run_extract_transcript_dag(
    track_id: str,
    audio_file_url: str,
    language: str,
    service: str,
    chunk_size: int,
    **kwargs
):
    current_datetime_string = datetime.now().strftime("%Y%m%d%H%M%S")
    dag_run_id = f"{track_id}_extract_transcript_{language}_{current_datetime_string}"
    trigger = TriggerDagRunOperator(
        task_id=f"trigger_target_dag",
        trigger_dag_id="dag_extract_transcript",
        trigger_run_id=dag_run_id,
        conf={
            "track_id": track_id,
            "url": audio_file_url,
            "language": language,
            "service": service,
            "chunk_size": chunk_size,
        },
        reset_dag_run=False,
        wait_for_completion=True,
    )
    trigger.execute(context=kwargs)
