import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
file_path_1 = '/opt/airflow/dags/ingestion_transactions.py'
file_path_2 = '/opt/airflow/dags/to_db_transactions.py'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 3, 15),
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'my_transactions_ingestion_dag',
    default_args=default_args,
    description='A DAG to perform ingestion of lenco transactions data to Postgres',
    schedule='0 23 * * *',
)

t1 = BashOperator(
    task_id='import_from_API',
    bash_command= f'python {file_path_1}',
    dag=dag,
)

t2 = BashOperator(
    task_id='export_to_database',
    bash_command= f'python {file_path_2}',
    dag=dag,
)

t1 >> t2