from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Constants
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
CSV_FILE = os.path.join(AIRFLOW_HOME, 'dags', 'ventes.csv')
API_SCRIPT = os.path.join(AIRFLOW_HOME, 'dags', 'api.py')

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# DAG definition
with DAG(
    dag_id='ventes_dag_csv_2',
    default_args=default_args,
    description='Generate and store sales data in CSV file',
    schedule_interval=timedelta(seconds=5),
    catchup=False,
    tags=['sales', 'csv'],
) as dag:

    # Tasks
    generate_usa_sales = BashOperator(
        task_id='generate_usa_sales',
        bash_command=f'python3 {API_SCRIPT} --generer_ventes_usa',
    )

    generate_france_sales = BashOperator(
        task_id='generate_france_sales',
        bash_command=f'python3 {API_SCRIPT} --generer_ventes_france',
    )

    save_to_csv = BashOperator(
        task_id='save_to_csv',
        bash_command=f'python3 {API_SCRIPT} --enregistrer_dans_csv',
    )

    # Task dependencies
    [generate_usa_sales, generate_france_sales] >> save_to_csv
