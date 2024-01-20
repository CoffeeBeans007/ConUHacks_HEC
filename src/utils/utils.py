import pandas as pd
import json
import csv
import os
from typing import List


def concat_json_to_csv(json_files: List[str], output_directory: str) -> str:
    """
    Concatenate JSON files and convert them into a single CSV file.

    Args:
    json_files (List[str]): List of JSON file paths to concatenate.
    output_directory (str): Directory path to save the output CSV file.

    Returns:
    str: Path of the created CSV file.
    """
    # Check and create output directory if not exists
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    concatenated_data = []

    for json_file in json_files:
        with open(json_file, 'r') as file:
            data = json.load(file)

            if not isinstance(data, list):
                raise ValueError(f"File {json_file} does not contain a valid JSON list.")

            concatenated_data.extend(data)

    output_csv_file = os.path.join(output_directory, 'exchange_concat.csv')

    with open(output_csv_file, 'w', newline='') as csv_file:
        if concatenated_data:
            writer = csv.DictWriter(csv_file, fieldnames=concatenated_data[0].keys())
            writer.writeheader()
            writer.writerows(concatenated_data)

    return output_csv_file


def read_data_csv(csv_path: str) -> pd.DataFrame:
    """
    Reads a CSV file and converts 'TimeStamp' and 'TimeStampEpoch' columns to datetime.

    Args:
    csv_path (str): Path to the CSV file.

    Returns:
    pd.DataFrame: DataFrame with converted datetime columns.
    """
    df = pd.read_csv(csv_path)

    # Convert 'TimeStamp' to datetime
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])

    # Convert 'TimeStampEpoch' to datetime
    df['TimeStampEpoch'] = pd.to_datetime(df['TimeStampEpoch'])

    return df


def find_n_random_tickers(df: pd.DataFrame, n: int) -> list[str]:
    return df['Symbol'].sample(n).tolist()


def filter_dataframe_by_tickers(df: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    return df[df['Symbol'].isin(tickers)]


if __name__ == "__main__":
    print("This is utils.py")

    json_files = ['../../data/Exchange_1.json', '../../data/Exchange_2.json', '../../data/Exchange_3.json']
    output_directory = '../../data'
    csv_path = '../../data/exchange_concat.csv'
    csv_file = concat_json_to_csv(json_files, output_directory)

    df = read_data_csv(csv_path=csv_path)
    print(df.head())

    n_random_tickers = find_n_random_tickers(df, 3)
    print(f"3 random tickers: \n{n_random_tickers}")

    filtered_df = filter_dataframe_by_tickers(df, n_random_tickers)
    print(f"First 10 rows of the filtered dataframe: \n{filtered_df.head(10)}")




