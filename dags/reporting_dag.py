from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Chemin absolu pour le fichier CSV
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
CSV_FILE = os.path.join(AIRFLOW_HOME, 'dags', 'ventes.csv')

# Adresse e-mail du responsable
RESPONSABLE_EMAIL = "adjaropatoussi@gmail.com"  # Remplacez par l'e-mail du responsable

def generer_rapport(**kwargs):
    """
    Génère un rapport contenant le meilleur vendeur et le produit le plus vendu.
    """
    try:
        # Charger les données de ventes depuis le fichier CSV
        if not os.path.exists(CSV_FILE):
            raise FileNotFoundError(f"Le fichier {CSV_FILE} n'existe pas.")
        
        df = pd.read_csv(CSV_FILE)
        
        # 1. Trouver le meilleur vendeur (celui avec le plus grand chiffre d'affaires)
        meilleur_vendeur = df.groupby('vendeur')['prix_total'].sum().idxmax()
        chiffre_affaires_meilleur_vendeur = df.groupby('vendeur')['prix_total'].sum().max()
        
        # 2. Trouver le produit le plus vendu (celui avec la plus grande quantité vendue)
        produit_plus_vendu = df.groupby('produit')['quantite_vendue'].sum().idxmax()
        quantite_produit_plus_vendu = df.groupby('produit')['quantite_vendue'].sum().max()
        
        # 3. Générer le rapport
        rapport = f"""
        Rapport des ventes :
        -------------------
        - Meilleur vendeur : {meilleur_vendeur} (Chiffre d'affaires : {chiffre_affaires_meilleur_vendeur:.2f} €)
        - Produit le plus vendu : {produit_plus_vendu} (Quantité vendue : {quantite_produit_plus_vendu})
        """
        
        # Enregistrer le rapport dans un fichier temporaire
        rapport_file = os.path.join(AIRFLOW_HOME, 'dags', 'rapport_ventes.txt')
        with open(rapport_file, 'w') as f:
            f.write(rapport)
        
        print("Rapport généré avec succès.")
        return rapport_file  # Retourne le chemin du fichier de rapport
    except Exception as e:
        print(f"Erreur lors de la génération du rapport : {e}")
        raise

# Définir les arguments par défaut du DAG
default_args = {
    'owner': 'airflow',
    'email': ['testsise5@gmail.com'],
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG
dag = DAG(
    'reporting_dag',
    default_args=default_args,
    description='DAG pour générer un rapport des ventes et l\'envoyer par e-mail',
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
    catchup=False,
)

# Tâche pour générer le rapport
tache_generer_rapport = PythonOperator(
    task_id='generer_rapport',
    python_callable=generer_rapport,
    provide_context=True,
    dag=dag,
)

# Tâche pour envoyer le rapport par e-mail
tache_envoyer_email = EmailOperator(
    task_id='envoyer_email',
    to=RESPONSABLE_EMAIL,
    subject='Rapport des ventes quotidien',
    html_content="""<p>Bonjour,</p>
                    <p>Veuillez trouver ci-joint le rapport des ventes quotidien.</p>
                    <p>Cordialement,</p>
                    <p>Votre équipe Airflow</p>""",
    files=["{{ ti.xcom_pull(task_ids='generer_rapport') }}"],  # Fichier joint
    dag=dag,
)

# Définir l'ordre des tâches
tache_generer_rapport >> tache_envoyer_email