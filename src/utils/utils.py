import pandas as pd
import json
import csv
import os

pd.set_option('display.max_columns', 10)
pd.set_option('display.max_rows', 10)


def concat_json_to_csv(json_files, output_directory):
    # Vérifie si le répertoire de sortie existe, sinon le crée
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Initialise une liste vide pour stocker les données JSON concaténées
    concatenated_data = []

    # Parcourt la liste des fichiers JSON
    for json_file in json_files:
        with open(json_file, 'r') as file:
            # Charge le fichier JSON
            data = json.load(file)

            # Assurez-vous que les données sont sous forme de liste de dictionnaires
            if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                concatenated_data.extend(data)
            else:
                raise ValueError(f"Le fichier {json_file} ne contient pas une liste de dictionnaires JSON valides.")

    # Crée le chemin complet pour le fichier de sortie CSV
    output_csv_file = os.path.join(output_directory, 'output.csv')

    # Écrit les données concaténées dans le fichier CSV
    with open(output_csv_file, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=concatenated_data[0].keys())

        # Écrit les en-têtes de colonnes
        writer.writeheader()

        # Écrit les données
        for row in concatenated_data:
            writer.writerow(row)

    return output_csv_file


def find_unique_tickers(df):
    return df['Symbol'].unique()


if __name__ == "__main__":
    print("This is utils.py")

    json_files = ['../../data/Exchange_1.json', '../../data/Exchange_2.json', '../../data/Exchange_3.json']
    output_directory = '../../data'

    # Concatène les fichiers JSON en un seul fichier CSV
    output_csv_file = concat_json_to_csv(json_files, output_directory)

