from datetime import datetime

from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@task(
    task_display_name="📑 Translate Transcript",
    map_index_template="{{ task.op_kwargs['language_to_translate_from'] ~ '->' ~ task.op_kwargs['language_to_translate_into'] }}")
def run_translate_transcript_dag(
    track_id: str,
    language_to_translate_from: str,
    language_to_translate_into: str,
    service: str,
    chunk_size: int,
    **kwargs
):
    current_datetime_string = datetime.now().strftime("%Y%m%d%H%M%S")
    dag_run_id = f"{track_id}_translate_transcript_{language_to_translate_from}_to_{language_to_translate_into}_{current_datetime_string}"
    trigger = TriggerDagRunOperator(
        task_id=f"trigger_target_dag",
        trigger_dag_id="dag_translate_transcript",
        trigger_run_id=dag_run_id,
        conf={
            "track_id": track_id,
            "language_to_translate_from": language_to_translate_from,
            "language_to_translate_into": language_to_translate_into,
            "service": service,
            "chunk_size": chunk_size,
        },
        reset_dag_run=False,
        wait_for_completion=True,
    )
    trigger.execute(context=kwargs)
