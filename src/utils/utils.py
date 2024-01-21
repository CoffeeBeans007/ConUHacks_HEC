import pandas as pd
import datetime
import time
import json
import csv
import os
from typing import List, Callable
import streamlit as st
import plotly.graph_objects as go


# from src.utils.file_manager import FileManagerDynamic
from utils.file_manager import FileManagerDynamic


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


def read_data_csv(folder_name: str, file_name: str) -> pd.DataFrame:
    """
    Reads a CSV file and converts 'TimeStamp' and 'TimeStampEpoch' columns to datetime.

    Args:
    csv_path (str): Path to the CSV file.

    Returns:
    pd.DataFrame: DataFrame with converted datetime columns.
    """

    fmd = FileManagerDynamic(ceiling_directory="30_TradingClub")

    df = fmd.load_data(folder_name=folder_name, file_name=file_name)

    # Convert 'TimeStamp' to datetime
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])

    # sort by timestamp
    df = df.sort_values(by=['TimeStamp'])

    return df


def find_n_random_tickers(df: pd.DataFrame, n: int, random_state: int = 42) -> list[str]:
    return df['Symbol'].sample(n, random_state=random_state).tolist()


def filter_dataframe_by_tickers(df: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    return df[df['Symbol'].isin(tickers)]


# Initialisation globale de la figure
message_type_colors = {
    'NewOrderRequest': 'blue',
    'NewOrderAcknowledged': 'green',
    'Cancelled': 'red',
    'CancelRequest': 'yellow',
    'CancelAcknowledged': 'purple',
    'Trade': 'orange',
    'Rejected': 'pink'
}

fig = go.Figure()
fig.update_layout(
    scene=dict(
        xaxis=dict(title='Time (seconds from start)', autorange="reversed"),
        yaxis_title='Symbol',
        zaxis_title='MessageType',
        camera=dict(
            eye=dict(x=2, y=2, z=2)  # Ajustez ces valeurs pour changer la vue initiale
        )
    ),
    title="MessageType over Time for each Symbol",
    legend_title="MessageType",
    autosize=True,  # Permet au graphique de s'ajuster à la taille du conteneur
    height=600     # Ajustez la hauteur du graphique
)



# Dictionary for mapping MessageType to symbols
message_type_symbols = {
    'NewOrderRequest': 'circle',
    'NewOrderAcknowledged': 'square',
    'Cancelled': 'diamond',
    'CancelRequest': 'cross',
    'CancelAcknowledged': 'x',
    'Trade': 'triangle-up',
    'Rejected': 'star'
}

# Placeholder for the graph in Streamlit
graph_placeholder = st.empty()


def processing_function(row: pd.Series, start_time: pd.Timestamp):
    global fig, graph_placeholder, message_type_symbols, message_type_colors
    time_diff = (row['TimeStamp'] - start_time).total_seconds()

    # Ajout de la trace pour la ligne actuelle à la figure existante
    fig.add_trace(go.Scatter3d(
        x=[time_diff], y=[row['Symbol']], z=[row['MessageType']],
        mode='markers',
        marker=dict(
            size=5,
            color=message_type_colors[row['MessageType']],
            symbol=message_type_symbols[row['MessageType']]
        ),
        name=row['MessageType']
    ))

    # Mise à jour du graphique dans le placeholder Streamlit
    graph_placeholder.plotly_chart(fig, use_container_width=True)


def display_dataframe_rows_over_time(df: pd.DataFrame, duration_minutes: int = 4):
    if df.empty:
        st.write("The DataFrame is empty.")
        return

    total_rows = len(df)
    total_seconds = duration_minutes * 60
    sleep_time = total_seconds / total_rows
    start_time = df['TimeStamp'].min()

    # Initialisation du placeholder pour le graphique dans Streamlit
    graph_placeholder = st.empty()

    for _, row in df.iterrows():
        processing_function(row, start_time)
        time.sleep(sleep_time)



def graph_dataframe_rows_over_time(df: pd.DataFrame, processing_function: Callable, duration_minutes: int = 4):
    """
    Processes each row of a DataFrame over a given period and allows calling a custom function at each step.

    Args:
    df (pd.DataFrame): DataFrame to process.
    processing_function: Custom function to call at each step.
    duration_minutes (int): Total duration for processing in minutes.
    """
    if df.empty:
        print("The DataFrame is empty.")
        return

    df['TimeStamps'] = pd.to_datetime(df['TimeStamp'])
    start_time = df['TimeStamps'].min()
    end_time = df['TimeStamps'].max()
    total_seconds = duration_minutes * 60
    total_rows = (end_time - start_time).total_seconds()
    sleep_time = total_seconds / total_rows

    graph_placeholder = st.empty()

    current_time = start_time
    while current_time < end_time:
        next_time = current_time + datetime.timedelta(seconds=1)
        processing_function(df, current_time, next_time, graph_placeholder)  # Call the custom processing function
        time.sleep(sleep_time)
        current_time = next_time


def process_data_per_second(df: pd.DataFrame, current_time: datetime.datetime):
    """
    Processes data for a given second and prints the total count of 'Trade' messages per 'Symbol'.

    Args:
    df (pd.DataFrame): The DataFrame containing the data.
    current_time (datetime.datetime): The current timestamp to process.
    """
    next_time = current_time + datetime.timedelta(seconds=1)
    filtered_df = df.query("MessageType == 'NewOrderRequest' and TimeStamp >= @current_time and TimeStamp < @next_time")
    count_per_symbol = filtered_df['Symbol'].value_counts()

    print(f"Time: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    if not count_per_symbol.empty:
        print("Trade count per Symbol:")
        print(count_per_symbol)
    else:
        print("No trades in this second.")


if __name__ == "__main__":
    print("This is utils.py")

    json_files = ['../../data/Exchange_1.json', '../../data/Exchange_2.json', '../../data/Exchange_3.json']
    # output_directory = '../../data'
    # csv_path = '../../data/exchange_concat.csv'
    # csv_file = concat_json_to_csv(json_files, output_directory)

    df = read_data_csv(file_name="exchange_concat.csv", folder_name="data")
    # print(df.head())
    #
    print(df["MessageType"].value_counts())
    # print(df.loc[df["MessageType"] == "Rejected"])
    # order_id = df['OrderID'].value_counts().sort_values(ascending=False)
    #
    # print(order_id[order_id == 1])
    #
    n_random_tickers = find_n_random_tickers(df, 1)
    # print(f"3 random tickers: \n{n_random_tickers}")
    #
    filtered_df = filter_dataframe_by_tickers(df, n_random_tickers)
    # print(f"First 10 rows of the filtered dataframe: \n{filtered_df.head(10)}")

    display_dataframe_rows_over_time(filtered_df, processing_function)



