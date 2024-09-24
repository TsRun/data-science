import os
import psycopg2
import pandas as pd
from datetime import datetime

# Connexion à la base de données
conn = psycopg2.connect(
    host="localhost",
    database="piscineds",
    user="maserrie",
    password="mysecretpassword"
)
cur = conn.cursor()

# Fonction pour générer le nom de la table à partir du nom du fichier CSV
def generate_table_name(file_path):
    base_name = os.path.basename(file_path)
    table_name = base_name.replace('.csv', '').replace('-', '_')
    return table_name

# Fonction pour nettoyer et préparer les données CSV avant de les copier dans PostgreSQL
def clean_and_prepare_csv(csv_path):
    df = pd.read_csv(csv_path)

    # Convertir la première colonne (event_time) en format datetime compatible avec PostgreSQL
    df['event_time'] = pd.to_datetime(df['event_time'], utc=True)

    # Sauvegarder les données nettoyées dans un fichier temporaire pour l'importation via COPY
    temp_csv_path = csv_path.replace('.csv', '_prepared.csv')
    df.to_csv(temp_csv_path, index=False)

    return temp_csv_path

# Fonction pour créer la table et insérer les données avec COPY
def create_table_and_copy_data(csv_path):
    # Nettoyage et préparation du CSV pour COPY
    prepared_csv_path = clean_and_prepare_csv(csv_path)

    # Définition des types de données
    column_types = [
        "event_time TIMESTAMP WITH TIME ZONE",   # Type 1 : TIMESTAMP WITH TIME ZONE
        "event_type TEXT",                       # Type 2 : TEXT
        "product_id INTEGER",                    # Type 3 : INTEGER
        "price FLOAT",                           # Type 4 : FLOAT
        "user_id BIGINT",                        # Type 5 : BIGINT
        "user_session UUID"                      # Type 6 : UUID
    ]

    # Création de la table
    table_name = generate_table_name(csv_path)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(column_types)}
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    # Utilisation de la commande COPY pour insérer les données
    with open(prepared_csv_path, 'r') as f:
        next(f)  # Skip the header row
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", f)
    conn.commit()

def main():

    # Chemin vers le fichier data_2022_oct.csv
    csv_path = "days00/subject/customer/data_2022_oct.csv"

    # Créer la table et copier les données via COPY
    create_table_and_copy_data(csv_path)

    # Fermeture de la connexion
    cur.close()
    conn.close()
    
if __name__ == "__main__":
    main()
