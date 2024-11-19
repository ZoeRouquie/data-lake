import pandas as pd
import os

# Définir le chemin de votre fichier CSV et le dossier de destination Parquet
input_csv_path = '/Users/madina/Desktop/Image/Road Accident Data.csv'  # Remplacez par le chemin de votre fichier CSV
output_parquet_folder = '/Users/madina/Desktop/datalake/parquet-accident/'  # Remplacez par le dossier où vous voulez enregistrer le fichier Parquet

# Créer le dossier de sortie si nécessaire
if not os.path.exists(output_parquet_folder):
    os.makedirs(output_parquet_folder)

# Lire le fichier CSV dans un DataFrame pandas
df = pd.read_csv(input_csv_path)

# Nom du fichier Parquet de sortie
parquet_file_name = os.path.basename(input_csv_path).replace(".csv", ".snappy.parquet")

# Sauvegarder le DataFrame en fichier Parquet avec compression Snappy
df.to_parquet(os.path.join(output_parquet_folder, parquet_file_name), engine='pyarrow')

print(f"Fichier {parquet_file_name} converti en Parquet avec succès!")
