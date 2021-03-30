from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import os


args = {
    'owner': 'airflow',
}

upload_dir = Variable.get('UPLOAD_DIR', '/opt/uploads')

def _already_processed(ti):
    if 2 > 1:
        return 'already_processed'
    else:
        return 'to_be_processed'

def load_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@yearly",
    )
    with dag_subdag:
        sd1 = BranchPythonOperator(
            task_id='is_already_processed',
            python_callable=_already_processed
        )
        already_processed = DummyOperator(
            task_id='already_processed'
        )
        to_be_processed = DummyOperator(
            task_id='to_be_processed'
        )
        geoprocessing = DummyOperator(
            task_id='geoprocessing'
        )
        sd1 >> [already_processed, to_be_processed] 
        to_be_processed >> geoprocessing
    return dag_subdag


def create_pipeline(dag_id,
               schedule,
               default_args):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        t1 = SubDagOperator(
            task_id='geoprocessing',
            subdag=load_subdag(dag_id,
                            'geoprocessing', default_args),
            default_args=default_args,
            dag=dag,
        )
        t4 = SimpleHttpOperator(
            task_id='update_layer',
            http_conn_id='geonode_conn_id',
            endpoint='geonode_endpoint',
            method='POST',
            data="{{ ti.xcom_pull(task_ids='geoprocessing')}}"
        )
        t5 = DummyOperator(task_id='pause_dag')

        t1 >> t4 >> t5

        t1 >> t4 >> t5

    return dag


for item in [i for i in os.listdir(upload_dir) if i.endswith('tif')]:
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2020, 3, 30)
    }
    globals()[item] = create_pipeline(f'process-{item}', '@yearly', default_args)