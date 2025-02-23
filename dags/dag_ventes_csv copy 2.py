# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import random
# import pandas as pd
# import os

# # Chemin absolu pour le fichier CSV
# AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
# CSV_FILE = os.path.join(AIRFLOW_HOME, 'dags', 'ventes.csv')

# # Créer le dossier data s'il n'existe pas
# os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)

# # Données prédéfinies pour les magasins
# magasins = {
#     "usa": {
#         "pays": "États-Unis",
#         "villes": ["New York", "Los Angeles"],
#         "noms_magasin": ["SuperMart USA", "QuickShop USA"],
#         "produits": {
#             "Pommes": 1.50,  # Prix en euros (simulation)
#             "Bananes": 0.80,
#             "Lait": 2.00,
#             "Pain": 1.20,
#             "Œufs": 2.50
#         },
#         "vendeurs": ["Alice", "Bob", "Charlie", "David", "Eve"]
#     },
#     "france": {
#         "pays": "France",
#         "villes": ["Paris", "Lyon"],
#         "noms_magasin": ["SuperMart France", "QuickShop France"],
#         "produits": {
#             "Pommes": 1.30,  # Prix en euros
#             "Bananes": 0.70,
#             "Lait": 1.80,
#             "Pain": 1.00,
#             "Œufs": 2.20
#         },
#         "vendeurs": ["Jean", "Marie", "Pierre", "Sophie", "Luc"]
#     }
# }

# def generer_vente(magasin_key):
#     """
#     Génère une vente pour chaque produit et chaque vendeur dans un magasin donné.
#     """
#     magasin = magasins[magasin_key]
#     ventes = []  # Liste pour stocker toutes les ventes générées
    
#     # Itérer sur chaque produit et chaque vendeur
#     for produit, prix_unitaire in magasin["produits"].items():
#         for vendeur in magasin["vendeurs"]:
#             # Choix aléatoires pour la ville et le nom du magasin
#             ville = random.choice(magasin["villes"])
#             nom_magasin = random.choice(magasin["noms_magasin"])
            
#             # Générer une quantité vendue aléatoire
#             quantite_vendue = random.randint(1, 10)
#             prix_total = quantite_vendue * prix_unitaire
            
#             # Ajouter la vente à la liste
#             ventes.append({
#                 "pays": magasin["pays"],
#                 "ville": ville,
#                 "nom_magasin": nom_magasin,
#                 "produit": produit,
#                 "prix_unitaire": prix_unitaire,
#                 "vendeur": vendeur,
#                 "quantite_vendue": quantite_vendue,
#                 "prix_total": prix_total
#             })
    
#     return ventes

# def generer_ventes_usa():
#     """
#     Génère des ventes pour le magasin aux États-Unis.
#     """
#     return generer_vente("usa")

# def generer_ventes_france():
#     """
#     Génère des ventes pour le magasin en France.
#     """
#     return generer_vente("france")

# def enregistrer_dans_csv(**kwargs):
#     """
#     Enregistre les données de vente dans un fichier CSV.
#     """
#     try:
#         # Récupérer les données de vente des deux magasins
#         ventes_usa = kwargs['ti'].xcom_pull(task_ids='generer_ventes_usa')
#         ventes_france = kwargs['ti'].xcom_pull(task_ids='generer_ventes_france')
        
#         # Combiner les ventes des deux magasins
#         ventes = ventes_usa + ventes_france
        
#         # Créer un DataFrame avec les données
#         df = pd.DataFrame(ventes)
        
#         # Vérifier si le fichier CSV existe déjà
#         if os.path.exists(CSV_FILE):
#             # Charger les données existantes et ajouter les nouvelles
#             df_existant = pd.read_csv(CSV_FILE)
#             df = pd.concat([df_existant, df], ignore_index=True)
        
#         # Enregistrer les données dans le fichier CSV
#         df.to_csv(CSV_FILE, index=False)
#         print(f"Données enregistrées dans {CSV_FILE}")
        
#         # Vérifier que le fichier a été créé
#         if os.path.exists(CSV_FILE):
#             print(f"Le fichier {CSV_FILE} a été créé avec succès.")
#         else:
#             print(f"Erreur : Le fichier {CSV_FILE} n'a pas été créé.")
#     except Exception as e:
#         print(f"Erreur lors de l'enregistrement des données : {e}")

# # Définir les arguments par défaut du DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 1, 1),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Définir le DAG
# dag = DAG(
#     'ventes_dag_csv',
#     default_args=default_args,
#     description='DAG pour générer et enregistrer les ventes des magasins dans un fichier CSV',
#     schedule_interval=timedelta(seconds=5),  # Exécution toutes les 5 secondes
#     catchup=False,
# )

# # Tâches pour générer les ventes
# tache_ventes_usa = PythonOperator(
#     task_id='generer_ventes_usa',
#     python_callable=generer_ventes_usa,
#     dag=dag,
# )

# tache_ventes_france = PythonOperator(
#     task_id='generer_ventes_france',
#     python_callable=generer_ventes_france,
#     dag=dag,
# )

# # Tâche pour enregistrer les données dans un fichier CSV
# tache_enregistrer_csv = PythonOperator(
#     task_id='enregistrer_dans_csv',
#     python_callable=enregistrer_dans_csv,
#     provide_context=True,
#     dag=dag,
# )

# # Définir l'ordre des tâches
# tache_ventes_usa >> tache_enregistrer_csv
# tache_ventes_france >> tache_enregistrer_csv