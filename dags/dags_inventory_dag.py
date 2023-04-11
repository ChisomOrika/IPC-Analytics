#dummy_dag.py
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
file_path_1 = '/opt/airflow/dags/to_db_customers.py'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 3, 21),
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'my_customer_ETL_dag',
    default_args=default_args,
    description='A DAG to perform ingestion of loyverse customer data to Postgres',
    schedule='0 */4 * * *',
)


t1 = BashOperator(
    task_id='from_API_to_DB_customers',
    bash_command= f'python {file_path_1}',
    dag=dag,
)

t1