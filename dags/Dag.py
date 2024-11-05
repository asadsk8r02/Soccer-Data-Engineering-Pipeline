from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('etl_arsenalfc', 
          default_args=default_args, 
          schedule_interval="@daily")

extract_task = BashOperator(
    task_id='extract',
    bash_command="spark-submit --jars /Drivers/SQL_Sever/jdbc/postgresql-42.7.3.jar /opt/airflow/spark/app/extract.py",
    dag=dag,
)

transform_load_task = BashOperator(
    task_id='transform_load',
    bash_command="spark-submit --jars /Drivers/SQL_Sever/jdbc/postgresql-42.7.3.jar /opt/airflow/spark/app/transform_load.py",
    dag=dag,
)

extract_task >> transform_load_task

