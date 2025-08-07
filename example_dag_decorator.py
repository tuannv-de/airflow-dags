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
    spark_app = {
        "apiVersion" : "sparkoperator.k8s.io/v1beta2",
        "kind" : "SparkApplication",
        "metadata" : {
            "name"      : "money-transfer-spark-streaming",
            "namespace" : "streaming-pipeline",
        },
        "spec" : {
            "type"               : "Scala",
            "pythonVersion"      : "3",
            "mode"               : "cluster",
            "image"              : "spark:3.5.3",
            "imagePullPolicy"    : "IfNotPresent",
            "mainClass"          : "org.apache.spark.examples.SparkPi",
            "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples.jar",
            "sparkVersion"       : "3.5.3",
            "arguments": ["5000"],

            "driver" : {
                "labels" : {
                    "version" : "3.5.3"
                },
                "serviceAccount" : "spark",
                "cores"          : 1,
                "memory"         : "1g",
                "env" : []
            },
            "executor" : {
                "labels" : {
                    "version" : "3.5.3"
                },
                "instances" : 1,
                "cores"     : 2,
                "memory"    : "512m",
                "env" : []
            }
        }
    }

    submit_spark = SparkKubernetesOperator(
        task_id='submit_spark_app',
        application=spark_app,
        kubernetes_conn_id='microk8s_default',  # Airflow connection to your microk8s cluster
        namespace='streaming-pipeline',
        do_xcom_push=True,
        get_logs=True,
        delete_on_termination=False,
    )

    # Task dependencies
    submit_spark
