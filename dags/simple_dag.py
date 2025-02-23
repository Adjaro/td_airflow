import logging
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'votre_nom',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'start_date': datetime.now(),
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='Mon premier DAG Airflow',
    schedule_interval=timedelta(days=1),
    catchup=False
)

tache_afficher_message = BashOperator(
    task_id='afficher_message',
    bash_command='echo "Bonjour, Airflow!"',
    dag=dag
)


def generer_message():
    logging.info("Exécution de la tâche Python")
    return "Tâche exécutée avec succès"

tache_python = PythonOperator(
    task_id='tache_python',
    python_callable=generer_message,
    dag=dag
)

# Définition des dépendances
tache_afficher_message >> tache_python