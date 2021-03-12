from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

project_cfg = {
    'owner': 'Allan Chepkoy',
    'email': ['allan.chepkoy@rakuten.com'],
    'description': 'A Basic Directed Acyclic graph with Hello world Python Tasks',
    'email_on_failure': True,
    'start_date': datetime(2021, 2, 22),
    'retries': 1,
    'retry_delay': timedelta(hours=1),
}

dag = DAG('basic_pipeline', default_args=project_cfg,
          schedule_interval=timedelta(days=1))


def example_task(_id, **kwargs):
    print("Task {}".format(_id))
    return "completed task {}".format(_id)


task_1 = PythonOperator(
    task_id='task_1',
    provide_context=True,
    python_callable=example_task,
    op_kwargs={'_id': 1},
    dag=dag
)

task_2 = PythonOperator(
    task_id='task_2',
    provide_context=True,
    python_callable=example_task,
    op_kwargs={'_id': 2},
    dag=dag
)

task_1 >> task_2
