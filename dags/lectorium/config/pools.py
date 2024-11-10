from airflow.models import Pool


Pool.create_or_update_pool(
    "vakshuddhi::process-audio",
    slots=4,
    description="Vakshuddhi pool",
    include_deferred=False)
