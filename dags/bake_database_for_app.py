from __future__ import annotations

from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator

from airflow.decorators import dag, task
from airflow.models import Variable
from docker.types import Mount

import lectorium as lectorium
import services.aws as aws


# ---------------------------------------------------------------------------- #
#                                      DAG                                     #
# ---------------------------------------------------------------------------- #

@dag(
    dag_display_name="📱 App: Bake Database for App",
    description="Prepares the database for distribution with the application",
    schedule='@daily',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["lectorium"],
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        "owner": "Advaita Krishna das",
    },
    render_template_as_native_obj=True,
    max_active_runs=1,
)
def bake_database_for_app():

    # ---------------------------------------------------------------------------- #
    #                                    Config                                    #
    # ---------------------------------------------------------------------------- #

    app_bucket_name = (
         Variable.get(lectorium.config.VAR_APP_BUCKET_NAME)
    )

    app_bucket_creds: lectorium.config.AppBucketAccessKey = (
        Variable.get(
            lectorium.config.VAR_APP_BUCKET_ACCESS_KEY,
            deserialize_json=True
        )
    )

    files = [
        'library-dictionary-v0001',
        'library-index-v0001',
        'library-tracks-v0001'
    ]


    # ---------------------------------------------------------------------------- #
    #                                     Steps                                    #
    # ---------------------------------------------------------------------------- #

    run_node_app = DockerOperator(
        auto_remove=True,
        mount_tmp_dir=False,
        task_id='bake_database',
        image='ghcr.io/akdasa-studios/lectorium-tools-bake-database',
        command='node index.js',
        docker_url='unix://var/run/docker.sock',
        network_mode='host',
        mounts=[
            Mount(source='/tmp/lectorium', target='/tools/artifacts', type='bind')
        ],
        environment={
            'DATABASE_URI': 'https://lectorium:!DVxqv-aj_wW4kCXC*TdbYk*EHk6rDqD@app.lectorium.akdasa.studio/database/',
        },
    )

    for file in files:
        uploaded_file = aws.upload_file(
            credentials=app_bucket_creds,
            bucket_name=app_bucket_name,
            object_key=f'artifacts/{file}.db',
            file_path=f'/tmp/lectorium/{file}.db',
        )

        run_node_app >> uploaded_file

bake_database_for_app()
