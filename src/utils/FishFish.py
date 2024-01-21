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
        
        
        
    def update_exchanges(self, existing_stats, new_row,firsttimestamp):
        '''
        Function to update the exchange stats and fish out trades that exceed 10 stdev of the average duration
        
        Args:
            existing_stats: Dictionary containing the exchange stats
            new_row: new row of the dataset
        Returns:
            existing_stats: Updated dictionary containing the exchange stats
        '''
        exchange = new_row['Exchange']
        order_id = new_row['OrderID']
        message_type = new_row['MessageType']
        timestamp = pd.to_datetime(new_row['TimeStamp'])
        new_row['TimeStamp']=pd.to_datetime(new_row['TimeStamp'])
        
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
        #Initilize the trade
        if message_type == 'NewOrderRequest':
            existing_stats[exchange]['Order Sent'] += 1
            existing_stats[exchange]['Open Orders'][order_id] = timestamp
        #Close the trade an update stats
        elif message_type in ['Cancelled','Rejected']:
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

        #Check each open order to see if it exceeds 10 stdev of the average duration
        for open_order_id, open_timestamp in existing_stats[exchange]['Open Orders'].items():
            open_duration = timestamp - open_timestamp
            open_duration_seconds = open_duration.total_seconds()
           
            threshold_seconds = 1 * existing_stats[exchange]['Duration StdDev'].total_seconds() + existing_stats[exchange]['Average Duration'].total_seconds()
            if open_duration_seconds > threshold_seconds and open_order_id not in existing_stats[exchange]['Flagged Trades'] and new_row['TimeStamp'] > firsttimestamp+pd.Timedelta(1,unit='m'):
                existing_stats[exchange]['Flagged Trades'].add(open_order_id)  #Add to set
        return existing_stats
    def novelSymbol(self,existing_SymbolCount,new_row,firsttimestamp):
        '''
        Function to check if the symbol has never been traded before
        Novelty data check
        
        Args:
            existing_SymbolCount: Dictionary containing the symbol count
            new_row: new row of the dataset
        Returns:
            existing_SymbolCount: Updated dictionary containing the symbol count
        '''
        
        exchange = new_row['Exchange']
        new_row['TimeStamp']=pd.to_datetime(new_row['TimeStamp'])
        instance=False
        if new_row['Symbol'] not in existing_SymbolCount[exchange]:
            #Initialize the symbol
            existing_SymbolCount[exchange][new_row['Symbol']]={}
            existing_SymbolCount[exchange][new_row['Symbol']]['HighestTimeDiff']=0
            existing_SymbolCount[exchange][new_row['Symbol']]['Count']=1
            existing_SymbolCount[exchange][new_row['Symbol']]['LastTradeTime']=new_row['TimeStampEpoch']

            existing_SymbolCount[exchange][new_row['Symbol']]['Threshold']=False
        
        elif new_row['MessageType']=='NewOrderRequest':
            time_diff = (new_row['TimeStampEpoch'] - existing_SymbolCount[exchange][new_row['Symbol']]['LastTradeTime'])
            existing_SymbolCount[exchange][new_row['Symbol']]['LastTradeTime']=new_row['TimeStampEpoch']

            existing_SymbolCount[exchange][new_row['Symbol']]['Count']+=1
            if time_diff>existing_SymbolCount[exchange][new_row['Symbol']]['HighestTimeDiff']:
                existing_SymbolCount[exchange][new_row['Symbol']]['HighestTimeDiff']=time_diff
                if existing_SymbolCount[exchange][new_row['Symbol']]['Count']>20:
                    existing_SymbolCount[exchange][new_row['Symbol']]['Threshold']=True
                    instance=True

        if new_row['TimeStamp'] > firsttimestamp+pd.Timedelta(1,unit='m') and instance and existing_SymbolCount[exchange][new_row['Symbol']]['Threshold'] and new_row['MessageType']=='NewOrderRequest':

            existing_SymbolCount[exchange]['Novelty'].add(new_row['Symbol'])
        
        
        return existing_SymbolCount
    def price_frequency(self,frequency_stats,new_row,granularity):
        '''
        Function to check the frequency of orders based on order type
        Args:
            frequency_stats: Dictionary containing the frequency stats
            new_row: new row of the dataset
            order_type: Type of order (NewOrderRequest,NewOrderAcknowledge,Trade,CancelRequest,CancelAcknowledged,Cancelled)
            granularity: Time interval to check frequency
        Returns:
            frequency_stats: Updated dictionary containing the frequency stats
        '''
        
       
        exchange = new_row['Exchange']
        new_row_time = pd.to_datetime(new_row['TimeStamp'])
        message_type = new_row['MessageType']

        interval_start = pd.to_datetime('2024-01-05 09:28:000000')
        interval_end = pd.to_datetime('2024-01-05 09:32:00.000000')

        if exchange not in frequency_stats:
            frequency_stats[exchange] = {'frequency': {}}

        if interval_start <= new_row_time <= interval_end:
            time_key = new_row_time.floor(granularity)
            if time_key not in frequency_stats[exchange]['frequency']:
                frequency_stats[exchange]['frequency'][time_key] = {'OrderCounts': {'NewOrderRequest':0, 'NewOrderAcknowledged':0, 'Cancelled':0, 'CancelRequest':0,
    'Trade':0, 'Rejected':0}}

            if message_type not in frequency_stats[exchange]['frequency'][time_key]['OrderCounts']:
                frequency_stats[exchange]['frequency'][time_key]['OrderCounts'][message_type] = 0

            frequency_stats[exchange]['frequency'][time_key]['OrderCounts'][message_type] += 1

        return frequency_stats



if __name__ == '__main__':
    
    #exchange1=pd.read_json('/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_1.json')
    #exchange2=pd.read_json('/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_2.json')
    #exchange3=pd.read_json('/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_3.json')
    #create_csv = Exchange.concat_json_to_csv(['/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_1.json','/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_2.json','/Users/jean-christophegaudreau/Downloads/National Bank Of Canada Data For ConUHacks VIII/Exchange_3.json'], '/Users/jean-christophegaudreau/Desktop/Coding/Python/ConUHackss')
    
    exchangeOrders=pd.read_csv('/Users/jean-christophegaudreau/Desktop/Coding/Python/ConUHackss/output.csv')
    startExchange=Exchange(exchangeOrders)
    #Initialize main dictionaries for every stats that we want to output
    
    
    exchange_stats = {
        'Exchange_1': {
                'Order Sent': 0,
                'Trade Passed': 0,
                'Order Cancelled': 0,
                'Open Orders': {},
                'Closed Durations': [],
                'Average Duration': pd.Timedelta(0),
                'Duration StdDev': pd.Timedelta(0),
                'Flagged Trades': set()  #Using a set to prevent duplicates and faster lookup
            },
        'Exchange_2': {
                'Order Sent': 0,
                'Trade Passed': 0,
                'Order Cancelled': 0,
                'Open Orders': {},
                'Closed Durations': [],
                'Average Duration': pd.Timedelta(0),
                'Duration StdDev': pd.Timedelta(0),
                'Flagged Trades': set()  #Using a set to prevent duplicates and faster lookup
            },
        'Exchange_3': {
                'Order Sent': 0,
                'Trade Passed': 0,
                'Order Cancelled': 0,
                'Open Orders': {},
                'Closed Durations': [],
                'Average Duration': pd.Timedelta(0),
                'Duration StdDev': pd.Timedelta(0),
                'Flagged Trades': set()  #Using a set to prevent duplicates and faster lookup
            }
    }
    #Structure for novelty stats (adding a new key for symbol in the function)
    existing_SymbolCount={
        'Exchange_1': {
                'Novelty': set(),
        },
        'Exchange_2': {
                'Novelty': set(),
               
        },
        'Exchange_3': {
                'Novelty': set(),
                
                
        }
    }
    
    #Structure for frequency stats (adding a new key for each timestamp as well as each order type in the function)
    frequency_stats={
        'Exchange_1': {
            'frequency':{}
        },
        'Exchange_2': {
            'frequency':{}

        },
        'Exchange_3': {
            'frequency':{}
        }
    }

    for index, row in exchangeOrders.iterrows():
        row_flagged = 0
        exchange_stats=startExchange.update_exchanges(exchange_stats,row,pd.to_datetime(exchangeOrders['TimeStamp'][0]))
        existing_SymbolCount=startExchange.novelSymbol(existing_SymbolCount,row,pd.to_datetime(exchangeOrders['TimeStamp'][0])) 
        frequency_stats=startExchange.price_frequency(frequency_stats,row,'1s')
        order_id = row['OrderID']
        print(exchange_stats)
        for exchange in exchange_stats:
            if order_id in exchange_stats[exchange]['Flagged Trades']:
                row_flagged = 1
                break  # No need to check further if already flagged

        # Check if the current row's Symbol is flagged in any exchange
        
        if not row_flagged:  # Only check if not already flagged by OrderID
            symbol = row['Symbol']
            for exchange in existing_SymbolCount:
                if symbol in existing_SymbolCount[exchange]['Novelty']:
                    row_flagged = 1
                    break  # No need to check further if already flagged

