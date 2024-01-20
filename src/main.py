import sys
import os
import time
import pandas as pd
import numpy as np
import json
import csv


class Exchange:
    def __init__(self, dataset):
        '''
        init function for the class
        dataset: timeseries order by timestamp
      
        Columns:
        
        TimeStamp: Time of the event
        TimeStampEpoch: Time of the event in epoch format
        Direction: NBF to Exchange or Exchange to NBF
        OrderID: Unique identifier for the order through time
        MessageType: Type of message (NewOrderRequest,NewOrderAcknowledge,Trade,CancelRequest,CancelAcknowledged,Cancelled
        Symbol: Symbol of Stock
        OrderPrice: Price of the order (except cancel trades)
        Exchange: Exchange where the order was sent
        '''
        self.dataset = dataset
        TimeStamp=dataset['TimeStamp']
        TimeStampEpoch=dataset['TimeStampEpoch']
        Direction=dataset['Direction']
        OrderID=dataset['OrderID']
        MessageType=dataset['MessageType']
        Symbol=dataset['Symbol']
        OrderPrice=dataset['OrderPrice']
        Exchange=dataset['Exchange']
        
        #Big Dictionary
        self.BigDict=dataset.to_dict(orient='index')
        
        
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
        
        # Sort the concatenated data by 'timestamp' column
        concatenated_data.sort(key=lambda x: (x['TimeStampEpoch']))
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
        
    def update_exchanges(self, existing_stats, new_row):
        exchange = new_row['Exchange']
        order_id = new_row['OrderID']
        message_type = new_row['MessageType']
        timestamp = pd.to_datetime(new_row['TimeStamp'])

        
        if exchange not in existing_stats:
            existing_stats[exchange] = {
                'Order Sent': 0,
                'Trade Passed': 0,
                'Order Cancelled': 0,
                'Open Orders': {},
                'Closed Durations': [],
                'Average Duration': pd.Timedelta(0),
                'Duration StdDev': pd.Timedelta(0),
                'Flagged Trades': set()#Sets are faster lol
            }

        if message_type == 'NewOrderRequest':
            existing_stats[exchange]['Order Sent'] += 1
            existing_stats[exchange]['Open Orders'][order_id] = timestamp
        elif message_type in ['CancelAcknowledged', 'Cancelled']:
            if order_id in existing_stats[exchange]['Open Orders']:
                start_timestamp = existing_stats[exchange]['Open Orders'][order_id]
                duration = timestamp - start_timestamp
                existing_stats[exchange]['Closed Durations'].append(duration.total_seconds())
                #Remove open orders once filled or cancelled
                del existing_stats[exchange]['Open Orders'][order_id]

        durations = existing_stats[exchange]['Closed Durations']
        if durations:
            average_duration = np.mean(durations)
            stddev_duration = np.std(durations, ddof=1)
            existing_stats[exchange]['Average Duration'] = pd.to_timedelta(average_duration, unit='s')
            existing_stats[exchange]['Duration StdDev'] = pd.to_timedelta(stddev_duration, unit='s')

        #Check each open order to see if it exceeds 2 stdev of the average duration
        for open_order_id, open_timestamp in existing_stats[exchange]['Open Orders'].items():
            open_duration = timestamp - open_timestamp
            open_duration_seconds = open_duration.total_seconds()

            threshold_seconds = 2 * existing_stats[exchange]['Duration StdDev'].total_seconds() + existing_stats[exchange]['Average Duration'].total_seconds()
            if open_duration_seconds > threshold_seconds and open_order_id not in existing_stats[exchange]['Flagged Trades']:
                existing_stats[exchange]['Flagged Trades'].add(open_order_id)  #Add to set
               
        return existing_stats
    






if __name__ == '__main__':
    
    #exchange1=pd.read_json('/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_1.json')
    #exchange2=pd.read_json('/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_2.json')
    #exchange3=pd.read_json('/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_3.json')
    #create_csv = Exchange.concat_json_to_csv(['/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_1.json','/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_2.json','/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_3.json'], '/Users/jean-christophegaudreau/Desktop/Coding/Python/ConUHackss')
    exchangeOrders=pd.read_csv('/Users/jean-christophegaudreau/Desktop/Coding/Python/ConUHackss/output.csv')
    startExchange=Exchange(exchangeOrders)
    exchange_stats = {
        'Exchange_1': {
                'Order Sent': 0,
                'Trade Passed': 0,
                'Order Cancelled': 0,
                'Open Orders': {},
                'Closed Durations': [],
                'Average Duration': pd.Timedelta(0),
                'Duration StdDev': pd.Timedelta(0),
                'Flagged Trades': set()  #Using a set to prevent duplicates
            },
        'Exchange_2': {
                'Order Sent': 0,
                'Trade Passed': 0,
                'Order Cancelled': 0,
                'Open Orders': {},
                'Closed Durations': [],
                'Average Duration': pd.Timedelta(0),
                'Duration StdDev': pd.Timedelta(0),
                'Flagged Trades': set()  #Using a set to prevent duplicates
            },
        'Exchange_3': {
                'Order Sent': 0,
                'Trade Passed': 0,
                'Order Cancelled': 0,
                'Open Orders': {},
                'Closed Durations': [],
                'Average Duration': pd.Timedelta(0),
                'Duration StdDev': pd.Timedelta(0),
                'Flagged Trades': set()  #Using a set to prevent duplicates
            }
    }
    for index, row in exchangeOrders.head(10).iterrows():
        exchange_stats=startExchange.update_exchanges(exchange_stats,row)
        print(exchange_stats['Exchange_1']['Flagged Trades'])
    

        




