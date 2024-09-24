import os
import psycopg2
import pandas as pd

# Connexion à la base de données
conn = psycopg2.connect(
    host="localhost",
    database="piscineds",
    user="maserrie",
    password="mysecretpassword"
)
cur = conn.cursor()

# Fonction pour créer la table "items" avec 3 types de données différents
def create_items_table(csv_path):
    # Définition des types de données
    column_types = [
        "product_id INTEGER",      # Type 1 : INTEGER
        "category_id BIGINT",      # Type 2 : BIGINT
        "category_code TEXT",      # Type 3 : TEXT
        "brand TEXT"               # Type 3 : TEXT
    ]

    # Création de la table "items"
    create_table_query = """
    CREATE TABLE IF NOT EXISTS items (
        {}
    );
    """.format(', '.join(column_types))
    
    cur.execute(create_table_query)
    conn.commit()

    # Nettoyage et préparation des données (conversion en entier pour category_id)
    df = pd.read_csv(csv_path)

    # Convertir category_id en entier (int) en traitant la notation scientifique
    df['category_id'] = pd.to_numeric(df['category_id'], errors='coerce').fillna(0).astype('int64')

    # Sauvegarder un fichier CSV temporaire pour COPY
    prepared_csv_path = csv_path.replace('.csv', '_prepared.csv')
    df.to_csv(prepared_csv_path, index=False)

    # Utilisation de COPY pour insérer les données dans la table
    with open(prepared_csv_path, 'r') as f:
        next(f)  # Skip the header row
        cur.copy_expert("COPY items FROM STDIN WITH CSV HEADER DELIMITER ','", f)
    
    conn.commit()

# Fonction principale
def main():
    # Chemin vers le fichier item.csv
    csv_path = "days00/subject/item/item.csv"

    # Créer la table et insérer les données
    create_items_table(csv_path)
    
    # Fermeture de la connexion
    cur.close()
    conn.close()

# Exécution du script
if __name__ == "__main__":
    main()

