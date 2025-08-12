from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago

import pendulum


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="spark-example-dag",
    schedule_interval="*/10 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['spark', 'k8s'],
) as dag:
    submit_spark = SparkKubernetesOperator(
        task_id='submit_spark_app',
        application_file="/voltemon/test/sparkapp.yaml",
        kubernetes_conn_id='kubernetes_default',  # Airflow connection to your microk8s cluster
        namespace='streaming-pipeline',
        get_logs=True,
        delete_on_termination=False,
    )

    # Task dependencies
    submit_spark
