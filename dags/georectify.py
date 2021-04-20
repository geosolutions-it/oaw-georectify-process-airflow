from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from custom_operator.georectify import GeoRectifyOperator
from custom_operator.geonode import GeoNodeUploaderOperator
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

        geonode_import = GeoNodeUploaderOperator(
            task_id="geonode_upload",
            default_args=default_args,
            file_to_upload=abs_filepath,
            output_dir=output_dir,
            dag=dag,
            filename=filename,
            custom_metadata="{{ ti.xcom_pull(task_ids='geoprocessing')}}",
            connection="geonode_conn_id"
        )

        georectify >> geonode_import

    return dag


tif_available = [timf for timf in os.listdir(upload_dir) if timf.endswith('.tif')]

for item in tif_available:

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
