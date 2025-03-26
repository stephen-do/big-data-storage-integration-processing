import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator


default_args = {
  'owner': '',
  'project': '',
  'retry_delay': timedelta(seconds=30),
  'retries': 6
}

with DAG('dag_name',
    start_date=pendulum.datetime(2024, 2, 1, tz="Asia/Ho_Chi_Minh"),
    schedule_interval=None,
    default_args = default_args,
    tags=["tag"]
) as dag:
    spark_job = SparkKubernetesOperator(
       task_id='task_id',
       application_file='app.yaml',
       namespace= "spark-operator",
       kubernetes_conn_id='k8s',
       delete_on_termination=True,
       do_xcom_push=False,
       dag=dag
   )
