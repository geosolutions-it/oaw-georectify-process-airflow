import os
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable

from custom_operator.geonode import GeoNodeUploaderOperator
from custom_operator.georectify import GeoRectifyOperator
from custom_operator.rename_file import FileRenameOperator


WAITING_DELAY_IN_MINUTES = Variable.get("WAITING_DELAY_IN_MINUTES", 3, deserialize_json=True)
OPERATOR_MAX_RETRIES = Variable.get("OPERATOR_MAX_RETRIES", 5, deserialize_json=True)

upload_dir = Variable.get("UPLOAD_DIR", "/opt/uploads")
output_dir = Variable.get("OUTPUT_DIR", "/opt/output")


def create_pipeline(dag_id, schedule, default_args, abs_filepath, filename):

    dag = DAG(
        dag_id,
        schedule_interval=schedule,
        default_args=default_args,
        concurrency=Variable.get("DAG_CONCURRENCY", 16, deserialize_json=True),
        max_active_runs=Variable.get("MAX_ACTIVE_RUNS", 1, deserialize_json=True),
    )

    with dag:
        georectify = GeoRectifyOperator(
            task_id="geoprocessing",
            abs_filepath=abs_filepath,
            default_args=default_args,
            output_folder=output_dir,
            dag=dag,
            filename=filename,
            retries=OPERATOR_MAX_RETRIES,
            retry_delay=timedelta(minutes=WAITING_DELAY_IN_MINUTES)
        )

        rename_file = FileRenameOperator(
            task_id="rename_file",
            default_args=default_args,
            file_to_upload=abs_filepath,
            output_dir=output_dir,
            prev_xcom="{{ ti.xcom_pull(task_ids='geoprocessing')}}",
            filename=filename,
        )

        geonode_import = GeoNodeUploaderOperator(
            task_id="geonode_upload",
            default_args=default_args,
            dag=dag,
            custom_metadata="{{ ti.xcom_pull(task_ids='rename_file')}}",
            connection="geonode_conn_id",
        )

        georectify >> rename_file >> geonode_import

    return dag


tif_available = [timf for timf in os.listdir(upload_dir) if timf.endswith(".tif")]

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
