from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os

directory_path = os.path.abspath(os.path.dirname(__file__))


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 25),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'hourly_pl_fixtures',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
)

get_data_task = BashOperator(
    task_id='get_data',
    bash_command='python3 {}/get_data_into_psql_dag.py'.format(directory_path),
    dag=dag
)

make_fixtures_task = BashOperator(
    task_id='make_fixtures_stats',
    bash_command='python3 {}/make_fixtures_pl_stats_dag.py'.format(directory_path),
    dag=dag
)

make_pdf_report_task = BashOperator(
    task_id='make_pdf_report',
    bash_command='python3 {}/make_pdf_report_pl_stats_dag.py'.format(directory_path),
    dag=dag
)

get_data_task >> make_fixtures_task >> make_pdf_report_task