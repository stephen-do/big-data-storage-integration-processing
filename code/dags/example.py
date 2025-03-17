from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Define default arguments
default_args = {
    "owner": "tuyendn",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    "example_dag",
    default_args=default_args,
    description="A simple example DAG",
    schedule_interval=None,  # Run once per day
    catchup=False,
)

# Python function for PythonOperator
def print_hello():
    print("Hello, Airflow!")

# Task 1: PythonOperator
task_1 = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello,
    dag=dag,
)

# Task 2: BashOperator
task_2 = BashOperator(
    task_id="print_date",
    bash_command="date",
    dag=dag,
)

# Task Dependencies
task_1 >> task_2  # task_1 runs before task_2
