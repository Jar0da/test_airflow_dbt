from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
import requests
import json
import psycopg2
import os

var_HOST_PC_IP = os.getenv("HOST_PC_IP")
var_DB_DEMO_USER = os.getenv("DB_DEMO_USER")
var_DB_DEMO_PASSWORD = os.getenv("DB_DEMO_PASSWORD")

# Configuration par défaut du pipeline (comme la gestion des erreurs sur Matillion)
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1), # Date de départ fictive
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# --- FONCTION 1 : INGESTION DE L'API ---
def extraire_donnees_api():
    url = "https://api.tcgdex.net/v2/fr/cards"
    print(f"Appel de l'API : {url}")
    
    response = requests.get(url)
    
    if response.status_code == 200:
        # On ne prend que les 1000 premières cartes pour l'exemple (l'API complète est énorme)
        donnees = response.json()[:1000]
        
        # On sauvegarde le JSON localement dans le conteneur
        chemin_fichier = "/opt/airflow/dags/cartes_pokemon.json"
        with open(chemin_fichier, 'w') as f:
            json.dump(donnees, f)
        print(f"Données sauvegardées avec succès ({len(donnees)} cartes).")
    else:
        raise Exception(f"Erreur API : Statut {response.status_code}")

# --- FONCTION 2 : CHARGEMENT DANS POSTGRES ---
def charger_dans_postgres():
    # 1. Lecture du fichier JSON généré par la tâche précédente
    with open("/opt/airflow/dags/cartes_pokemon.json", 'r') as f:
        cartes = json.load(f)
        
    # 2. Connexion à notre BDD (Réseau Docker -> nom du service 'db_postgres')
    conn = psycopg2.connect(
        host=var_HOST_PC_IP,
        port="5434",
        database="db_demo",
        user=var_DB_DEMO_USER,
        password=var_DB_DEMO_PASSWORD
    )
    cursor = conn.cursor()
    
    # 3. Création de la table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stg.stg_cartes_pokemon (
            id VARCHAR(50) PRIMARY KEY,
            nom VARCHAR(255),
            image_url TEXT
        );
    """)
    
    # 4. Insertion des données (avec gestion des doublons avec ON CONFLICT)
    for carte in cartes:
        # L'API fournit 'id', 'name', et parfois 'image'
        id_carte = carte.get('id')
        nom_carte = carte.get('name')
        image_carte = f"{carte.get('image')}/low.png" if carte.get('image') else None
        
        cursor.execute("""
            INSERT INTO stg.stg_cartes_pokemon (id, nom, image_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET nom = EXCLUDED.nom, image_url = EXCLUDED.image_url;
        """, (id_carte, nom_carte, image_carte))
        
    conn.commit()
    cursor.close()
    conn.close()
    print("Insertion dans PostgreSQL terminée avec succès !")


# --- DÉFINITION DU DAG (LE PIPELINE) ---
with DAG(
    'pipeline_pokemon_api',
    default_args=default_args,
    description='Pipeline de récupération des cartes Pokémon via API',
    schedule_interval=None, # Déclenchement manuel pour nos tests
    catchup=False
) as dag:

    # Tâche 1 : Récupérer les données
    task_extraction = PythonOperator(
        task_id='extraire_depuis_api',
        python_callable=extraire_donnees_api
    )

    # Tâche 2 : Charger dans le Data Warehouse (Postgres)
    task_chargement = PythonOperator(
        task_id='charger_dans_postgres',
        python_callable=charger_dans_postgres
    )

    # L'ordonnancement (La flèche sur Matillion)
    task_extraction >> task_chargement