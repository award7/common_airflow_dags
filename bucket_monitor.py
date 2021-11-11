from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from operators.trigger_multi_dagrun import TriggerMultiDagRunOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime
import os


def _check_for_contents(*, path: str, **kwargs) -> bool:
    if os.listdir(path):
        return True
    return False


def _trigger_dagrun_helper(*, path: str, **kwargs) -> None:
    directory_contents = os.listdir(path)
    if directory_contents:
        for item in directory_contents:
            yield{'payload': item}


default_args = {

}
# todo: change to '@daily'
with DAG(dag_id='bucket-monitor', default_args=default_args, schedule_interval=None, start_date=datetime(2021, 11, 11), catchup=False) as DAG:
    with TaskGroup(group_id='asl') as asl_tg:
        asl_bucket_monitor = ShortCircuitOperator(
            task_id='asl-bucket-monitor',
            python_callable=_check_for_contents,
            op_kwargs={
                'path': "{{ var.value.asl_raw_path }}"
            }
        )

        trigger_asl_dags = TriggerMultiDagRunOperator(
            task_id='trigger-asl-dags',
            trigger_dag_id='asl-main-dag',
            python_callable=_trigger_dagrun_helper,
            op_kwargs={
                'path': "{{ var.value.asl_raw_path }}"
            },
            wait_for_completion=False
        )
        asl_bucket_monitor >> trigger_asl_dags

    with TaskGroup(group_id='hypercapnia') as hypercapnia_tg:
        hypercapnia_bucket_monitor = ShortCircuitOperator(
            task_id='hypercapnia-bucket-monitor',
            python_callable=_check_for_contents,
            op_kwargs={
                'path': "{{ var.value.bucket_path }}/hypercapnia"
            }
        )

        trigger_hypercapnia_dags = TriggerMultiDagRunOperator(
            task_id='trigger-hypercapnia-dags',
            trigger_dag_id='hypercapnia-processing',
            python_callable=_trigger_dagrun_helper,
            op_kwargs={
                'path': "{{ var.value.asl_raw_path }}"
            },
            wait_for_completion=False
        )
        hypercapnia_bucket_monitor >> trigger_hypercapnia_dags


