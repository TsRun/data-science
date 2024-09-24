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

# Fonction pour parcourir tous les fichiers CSV du dossier customer et les traiter
def process_all_csv_in_customer_folder():
    base_dir = "days00/subject/customer"  # Chemin vers le dossier 'customer'
    
    # Parcourir tous les fichiers CSV dans le dossier 'customer'
    for file_name in os.listdir(base_dir):
        if file_name.endswith('.csv'):
            csv_path = os.path.join(base_dir, file_name)
            print(f"Processing file: {file_name}")
            create_table_and_copy_data(csv_path)

def main():
    # Traiter tous les fichiers CSV du dossier 'customer'
    process_all_csv_in_customer_folder()

    # Fermeture de la connexion
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()