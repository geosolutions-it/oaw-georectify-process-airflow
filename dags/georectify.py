from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from custom_operator.georectify import GeoRectifyOperator
import re
import os


args = {
    'owner': 'airflow',
}

upload_dir = Variable.get('UPLOAD_DIR', '/opt/uploads')
output_dir = Variable.get('OUTPUT_DIR', '/opt/output')
geonode_endpoint = Variable.get('GEONODE_ENDPOINT', 'localhost')


def create_pipeline(dag_id,
               schedule,
               default_args,
               abs_filepath):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        georectify = GeoRectifyOperator(
            task_id='geoprocessing',
            abs_filepath=abs_filepath,
            default_args=default_args,
            output_folder=output_dir,
            dag=dag,
        )
        update_layers = SimpleHttpOperator(
            task_id='update_layer',
            http_conn_id='geonode_conn_id',
            endpoint=geonode_endpoint,
            method='GET',
            data="{{ ti.xcom_pull(task_ids='geoprocessing')}}"
        )

        georectify >> update_layers

    return dag


for item in [i for i in os.listdir(upload_dir) if i.endswith('tif') and '_fin' not in i]:
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2020, 3, 30)
    }
    name = re.sub('\.tif$', '', item)
    abs_filepath = f"{upload_dir}/{item}"
    globals()[item] = create_pipeline(f'{name}', '@once', default_args, abs_filepath)
