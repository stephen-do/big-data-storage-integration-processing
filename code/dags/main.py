import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

default_args = {
    'owner': '',
    'project': '',
    'retry_delay': timedelta(seconds=30),
    'retries': 0
}

with DAG('main_pipeline_for_stock_data_dag',
         start_date=pendulum.datetime(2024, 2, 1, tz="Asia/Ho_Chi_Minh"),
         schedule_interval=None,
         default_args=default_args,
         tags=["tag"]
         ) as dag:
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')
    with TaskGroup("stock_jobs") as stock_jobs:
        golden_stock_index_rsi = SparkKubernetesOperator(
            task_id='golden_stock_index_rsi',
            application_file='app.yaml',
            namespace="spark",
            kubernetes_conn_id='k8s',
            delete_on_termination=True,
            do_xcom_push=False,
            params={
                "job": "golden_stock_index_rsi",
                "driver_core": '1',
                "driver_mem": '512m',
                "executor_ins": '1',
                "executor_core": '1',
                "executor_mem": '512m',
            },
            dag=dag
        )
        golden_stock_index_ema = SparkKubernetesOperator(
            task_id='golden_stock_index_ema',
            application_file='app.yaml',
            namespace="spark",
            kubernetes_conn_id='k8s',
            delete_on_termination=True,
            do_xcom_push=False,
            params={
                "job": "golden_stock_index_ema",
                "driver_core": '1',
                "driver_mem": '512m',
                "executor_ins": '1',
                "executor_core": '1',
                "executor_mem": '512m',
            },
            dag=dag
        )
        interface_stock_overview = SparkKubernetesOperator(
            task_id='interface_stock_overview',
            application_file='app.yaml',
            namespace="spark",
            kubernetes_conn_id='k8s',
            delete_on_termination=True,
            do_xcom_push=False,
            params={
                "job": "interface_stock_overview",
                "driver_core": '1',
                "driver_mem": '1g',
                "executor_ins": '1',
                "executor_core": '1',
                "executor_mem": '1g',
            },
            dag=dag
        )
        [golden_stock_index_rsi, golden_stock_index_ema] >> interface_stock_overview
    with TaskGroup("industry_jobs") as industry_jobs:
        golden_industry_index_ema = SparkKubernetesOperator(
            task_id='golden_industry_index_ema',
            application_file='app.yaml',
            namespace="spark",
            kubernetes_conn_id='k8s',
            delete_on_termination=True,
            do_xcom_push=False,
            params={
                "job": "golden_industry_index_ema",
                "driver_core": '1',
                "driver_mem": '512m',
                "executor_ins": '1',
                "executor_core": '1',
                "executor_mem": '512m',
            },
            dag=dag
        )
        interface_industry_overview = SparkKubernetesOperator(
            task_id='interface_industry_overview',
            application_file='app.yaml',
            namespace="spark",
            kubernetes_conn_id='k8s',
            delete_on_termination=True,
            do_xcom_push=False,
            params={
                "job": "interface_industry_overview",
                "driver_core": '1',
                "driver_mem": '512m',
                "executor_ins": '1',
                "executor_core": '1',
                "executor_mem": '512m',
            },
            dag=dag
        )
        golden_industry_index_ema >> interface_industry_overview
    start_task >> stock_jobs >> industry_jobs >> end_task
