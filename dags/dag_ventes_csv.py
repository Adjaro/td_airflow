from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import random
import pandas as pd
import os

# Configuration
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data')
CSV_FILE = os.path.join(DATA_DIR, 'ventes_transformed.csv')
os.makedirs(DATA_DIR, exist_ok=True)

# Taux de conversion (simulé)
TAUX_CONVERSION = {
    'EUR_TO_GBP': 0.85,  # 1 EUR = 0.85 GBP
    'USD_TO_GBP': 0.79   # 1 USD = 0.79 GBP
}

# Données des magasins avec prix en devise locale
magasins = {
    "usa": {
        "pays": "États-Unis",
        "devise": "USD",
        "villes": ["New York", "Los Angeles"],
        "noms_magasin": ["SuperMart USA", "QuickShop USA"],
        "produits": {
            "Pommes": 1.80,  # Prix en USD
            "Bananes": 0.95,
            "Lait": 2.40,
            "Pain": 1.45,
            "Œufs": 3.00
        },
        "vendeurs": ["Alice", "Bob", "Charlie", "David", "Eve"]
    },
    "france": {
        "pays": "France",
        "devise": "EUR",
        "villes": ["Paris", "Lyon"],
        "noms_magasin": ["SuperMart France", "QuickShop France"],
        "produits": {
            "Pommes": 1.30,  # Prix en EUR
            "Bananes": 0.70,
            "Lait": 1.80,
            "Pain": 1.00,
            "Œufs": 2.20
        },
        "vendeurs": ["Jean", "Marie", "Pierre", "Sophie", "Luc"]
    }
}

def extraction_ventes(magasin_key):
    """
    Extrait les données de vente pour un magasin.
    """
    magasin = magasins[magasin_key]
    ventes = []
    
    for produit, prix_unitaire in magasin["produits"].items():
        for vendeur in magasin["vendeurs"]:
            ville = random.choice(magasin["villes"])
            nom_magasin = random.choice(magasin["noms_magasin"])
            quantite_vendue = random.randint(1, 10)
            prix_total = quantite_vendue * prix_unitaire
            
            ventes.append({
                "pays": magasin["pays"],
                "devise_origine": magasin["devise"],
                "ville": ville,
                "nom_magasin": nom_magasin,
                "produit": produit,
                "prix_unitaire_original": prix_unitaire,
                "vendeur": vendeur,
                "quantite_vendue": quantite_vendue,
                "prix_total_original": prix_total,
                "date_vente": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
    
    return ventes

def transformation_ventes(ventes, pays):
    """
    Transforme les données de vente en convertissant les prix en GBP.
    """
    ventes_transformees = []
    for vente in ventes:
        vente_transformee = vente.copy()
        
        # Sélectionner le taux de conversion approprié
        taux = (TAUX_CONVERSION['EUR_TO_GBP'] 
                if vente['devise_origine'] == 'EUR' 
                else TAUX_CONVERSION['USD_TO_GBP'])
        
        # Convertir les prix en GBP
        vente_transformee['prix_unitaire_gbp'] = round(vente['prix_unitaire_original'] * taux, 2)
        vente_transformee['prix_total_gbp'] = round(vente['prix_total_original'] * taux, 2)
        vente_transformee['taux_conversion'] = taux
        vente_transformee['date_transformation'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        ventes_transformees.append(vente_transformee)
    
    print(f"✓ Transformation des données de {pays} terminée")
    return ventes_transformees

# Tâches ETL
def extract_usa():
    """Extraction des données USA"""
    return extraction_ventes("usa")

def extract_france():
    """Extraction des données France"""
    return extraction_ventes("france")

def transform_usa(**context):
    """Transformation des données USA"""
    ventes_usa = context['task_instance'].xcom_pull(task_ids='extract_usa')
    return transformation_ventes(ventes_usa, "USA")

def transform_france(**context):
    """Transformation des données France"""
    ventes_france = context['task_instance'].xcom_pull(task_ids='extract_france')
    return transformation_ventes(ventes_france, "France")

def load_data(**context):
    """Chargement des données transformées"""
    try:
        # Récupérer les données transformées
        ventes_usa = context['task_instance'].xcom_pull(task_ids='transform_usa')
        ventes_france = context['task_instance'].xcom_pull(task_ids='transform_france')
        
        # Combiner les données
        toutes_ventes = ventes_usa + ventes_france
        
        # Créer le DataFrame
        df = pd.DataFrame(toutes_ventes)
        
        # Gérer le fichier existant
        if os.path.exists(CSV_FILE):
            df_existant = pd.read_csv(CSV_FILE)
            df = pd.concat([df_existant, df], ignore_index=True)
        
        # Sauvegarder
        df.to_csv(CSV_FILE, index=False)
        print(f"✓ Données chargées dans {CSV_FILE}")
        print(f"✓ Nombre total d'enregistrements: {len(df)}")
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement: {str(e)}")
        raise

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Création du DAG
dag = DAG(
    'etl_ventes_pipeline',
    default_args=default_args,
    description='Pipeline ETL: Extraction → Transformation (conversion en GBP) → Chargement',
    schedule_interval=timedelta(seconds=5),
    catchup=False,
)

# Tâches d'extraction
extract_usa_task = PythonOperator(
    task_id='extract_usa',
    python_callable=extract_usa,
    dag=dag,
)

extract_france_task = PythonOperator(
    task_id='extract_france',
    python_callable=extract_france,
    dag=dag,
)

# Tâches de transformation
transform_usa_task = PythonOperator(
    task_id='transform_usa',
    python_callable=transform_usa,
    provide_context=True,
    dag=dag,
)

transform_france_task = PythonOperator(
    task_id='transform_france',
    python_callable=transform_france,
    provide_context=True,
    dag=dag,
)

# Tâche de chargement
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Définition du flux de données
extract_usa_task >> transform_usa_task >> load_task
extract_france_task >> transform_france_task >> load_taska