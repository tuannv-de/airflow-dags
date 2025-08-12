from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

import pendulum
import httpx


def get_request(**kwargs):
    url = kwargs['url']
    response = httpx.get(url).json()
    # Push the IP address into XCom
    kwargs['ti'].xcom_push(key='origin_ip', value=response['origin'])


def prepare_command(**kwargs):
    origin_ip = kwargs['ti'].xcom_pull(key='origin_ip')
    return f"echo 'Seems like today your server executing Airflow is connected from IP {origin_ip}'"


def demo_metric(**kwargs):
    # This simple Python task can be used to demo metric collection (e.g., StatsD)
    # You could integrate StatsD here; for demo, we just log a message.
    print("Demo metric task executed")

with DAG(
    dag_id="example21112",
    schedule_interval="*/4 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:

    # Task: retrieve origin IP via HTTP
    get_ip = PythonOperator(
        task_id="get_ip",
        python_callable=get_request,
        op_kwargs={"url": "http://httpbin.org/get"},
    )

    # Task: prepare the echo command using pulled XCom
    command_task = PythonOperator(
        task_id="prepare_command",
        python_callable=prepare_command,
    )

    # Task: execute the echo command
    echo_task = BashOperator(
        task_id="echo_ip_info",
        bash_command="{{ task_instance.xcom_pull(task_ids='prepare_command') }}",
    )

    # New simple demo metric task
    metric_demo = PythonOperator(
        task_id="demo_metric_task",
        python_callable=demo_metric,
    )

    # Define task order
    get_ip >> command_task >> echo_task >> metric_demo
