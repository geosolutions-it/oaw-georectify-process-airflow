from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from custom_operator.georectify import GeoRectifyOperator
import re
import os
import json


args = {
    "owner": "airflow",
}

upload_dir = Variable.get("UPLOAD_DIR", "/opt/uploads")
output_dir = Variable.get("OUTPUT_DIR", "/opt/output")


def create_pipeline(dag_id, schedule, default_args, abs_filepath, filename):

    dag = DAG(
        dag_id, schedule_interval=schedule, default_args=default_args, max_active_runs=1
    )

    with dag:
        georectify = GeoRectifyOperator(
            task_id="geoprocessing",
            abs_filepath=abs_filepath,
            default_args=default_args,
            output_folder=output_dir,
            dag=dag,
            filename=filename
        )
        update_layers = SimpleHttpOperator(
            task_id="update_layer",
            http_conn_id="geonode_conn_id",
            endpoint="api/management/importlayers/",
            method="POST",
            headers={"Content-Type": "application/json"},
            data="{{ ti.xcom_pull(task_ids='geoprocessing')}}",
            retry_delay=10
        )

        georectify >> update_layers

    return dag


tif_available = [timf for timf in os.listdir(upload_dir) if timf.endswith('.tif')]
tif_to_process =  [timf for timf in tif_available if os.path.exists(f'{upload_dir}/{timf}.points')]

for item in tif_to_process:

    default_args = {"owner": "airflow", "start_date": datetime(2020, 3, 30)}

    name = re.sub("\.tif$", "", item)

    abs_filepath = f"{upload_dir}/{item}"

    globals()[item] = create_pipeline(
        dag_id=f"{name}",
        schedule="@once",
        default_args=default_args,
        abs_filepath=abs_filepath,
        filename=item,
    )
