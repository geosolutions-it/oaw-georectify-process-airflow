from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
from custom_operator.georectify import GeoRectifyOperator
import os


args = {
    'owner': 'airflow',
}

upload_dir = Variable.get('UPLOAD_DIR', '/opt/uploads')
geonode_endpoint = Variable.get('GEONODE_ENDPOINT', 'localhost:8000')


def create_pipeline(dag_id,
               schedule,
               default_args):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        t1 = GeoRectifyOperator(
            task_id='geoprocessing',
            filename=dag_id,
            default_args=default_args,
            dag=dag,
        )
        t4 = SimpleHttpOperator(
            task_id='update_layer',
            http_conn_id='geonode_conn_id',
            endpoint=geonode_endpoint,
            method='POST',
            data="{{ ti.xcom_pull(task_ids='geoprocessing')}}"
        )

        t1 >> t4

    return dag


for item in [i for i in os.listdir(upload_dir) if i.endswith('tif')]:
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2020, 3, 30)
    }
    globals()[item] = create_pipeline(f'{item}', '@once', default_args)