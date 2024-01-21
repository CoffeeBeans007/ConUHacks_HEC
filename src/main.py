import pandas as pd
import streamlit as st
st.set_page_config(layout="wide")
from utils.filter_data import FilterData
from utils.find_patterns import FindPatterns
from utils.file_manager import FileManagerDynamic
from FishFish import Exchange
from utils.utils import display_data_3d_over_time, processing_function


def main_fish(row: pd.Series):
    # Read the data
    fms = FileManagerDynamic(ceiling_directory='30_TradingClub')
    exchangeOrders = fms.load_data(folder_name='data', file_name='exchange_concat.csv')
    startExchange = Exchange(exchangeOrders)
    # Initialize main dictionaries for every stats that we want to output

    exchange_stats = {
        'Exchange_1': {
            'Order Sent': 0,
            'Trade Passed': 0,
            'Order Cancelled': 0,
            'Open Orders': {},
            'Closed Durations': [],
            'Average Duration': pd.Timedelta(0),
            'Duration StdDev': pd.Timedelta(0),
            'Flagged Trades': set()  # Using a set to prevent duplicates and faster lookup
        },
        'Exchange_2': {
            'Order Sent': 0,
            'Trade Passed': 0,
            'Order Cancelled': 0,
            'Open Orders': {},
            'Closed Durations': [],
            'Average Duration': pd.Timedelta(0),
            'Duration StdDev': pd.Timedelta(0),
            'Flagged Trades': set()  # Using a set to prevent duplicates and faster lookup
        },
        'Exchange_3': {
            'Order Sent': 0,
            'Trade Passed': 0,
            'Order Cancelled': 0,
            'Open Orders': {},
            'Closed Durations': [],
            'Average Duration': pd.Timedelta(0),
            'Duration StdDev': pd.Timedelta(0),
            'Flagged Trades': set()  # Using a set to prevent duplicates and faster lookup
        }
    }
    # Structure for novelty stats (adding a new key for symbol in the function)
    existing_SymbolCount = {
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

    # Structure for frequency stats (adding a new key for each timestamp as well as each order type in the function)
    frequency_stats = {
        'Exchange_1': {
            'frequency': {}
        },
        'Exchange_2': {
            'frequency': {}

        },
        'Exchange_3': {
            'frequency': {}
        }
    }

    row_flagged = False
    exchange_stats = startExchange.update_exchanges(exchange_stats, row,
                                                    pd.to_datetime(exchangeOrders['TimeStamp'][0]))
    existing_SymbolCount = startExchange.novelSymbol(existing_SymbolCount, row,
                                                     pd.to_datetime(exchangeOrders['TimeStamp'][0]))
    frequency_stats = startExchange.price_frequency(frequency_stats, row, '1s')
    print(exchange_stats)
    if row["OrderID"] in exchange_stats[row["Exchange"]]['Flagged Trades']:
        row["RowFlagged"] = 1

    # Check if the current row's Symbol is flagged in any exchange

    if not row_flagged:  # Only check if not already flagged by OrderID
        symbol = row['Symbol']
        for exchange in existing_SymbolCount:
            if symbol in existing_SymbolCount[exchange]['Novelty']:
                row["RowFlagged"] = 1
                break  # No need to check further if already flagged


def get_number_input(filter_name, default_n=1):
    return st.number_input(f"Number of tickers for {filter_name}", min_value=1, value=default_n)


def get_text_input(filter_name, default_text=''):
    return st.text_input(f"Enter tickers for {filter_name}", value=default_text)


def configure_filters(df):
    st.title("Filter Configuration")

    order_id_to_pattern_dict = FindPatterns(df).map_order_id_to_pattern()
    df['PatternID'] = df['OrderID'].map(order_id_to_pattern_dict)
    # apply main_fish by row to create a new column

    selected_exchanges = st.multiselect("Select Exchange(s):", df['Exchange'].unique(), default=df['Exchange'].unique())
    df_exchanges_filtered = FilterData(df).filter_by_exchanges(selected_exchanges)

    selected_symbols = st.multiselect("Select Symbols:", df_exchanges_filtered['Symbol'].unique(), key='symbols', default=df_exchanges_filtered['Symbol'].unique())
    df_symbols_filtered = FilterData(df_exchanges_filtered).filter_by_ticker_list(selected_symbols)

    filter_type = st.selectbox("Select filter type:", ["Top Tickers by MessageType", "Top Tickers by Order Count", "Filter by Patterns"], index=2)

    if filter_type == "Filter by Patterns":
        find_patterns = FindPatterns(df_symbols_filtered)
        pattern_full_counts, pattern_mapping = find_patterns.find_and_count_patterns()
        patterns_sorted = find_patterns.replace_pattern_keys(pattern_full_counts, pattern_mapping)

        df_to_show = pd.DataFrame(list(patterns_sorted.items()), columns=['Sequence', 'Pattern Count'])
        df_to_show['PatternID'] = df_to_show['Sequence'].map(pattern_mapping)

        st.write(df_to_show)

        selected_patterns_keys = st.multiselect("Select Patterns:", list(patterns_sorted.keys()))
        selected_sequences = [key.split(' -> ') for key in selected_patterns_keys]
        df_filtered = FilterData(df_symbols_filtered).filter_by_message_type_sequence(selected_sequences)
    else:
        n = st.number_input(f"Enter number of top tickers by {filter_type.lower()}:")
        if filter_type == "Top Tickers by MessageType":
            df_filtered = FilterData(df_symbols_filtered).get_top_tickers_by_message_type(n)
        elif filter_type == "Top Tickers by Order Count":
            df_filtered = FilterData(df_symbols_filtered).get_top_tickers_by_order_count(n)

    if st.button("Apply Filter"):
        st.session_state['filter_applied'] = True
        st.session_state['filtered_df'] = df_filtered
        # st.write(df_filtered)


def main():
    st.title("QuantExplorerApplication")
    st.write("This is the main page")

    fms = FileManagerDynamic(ceiling_directory='30_TradingClub')
    df = fms.load_data(folder_name='data', file_name='exchange_concat.csv')

    if 'filter_applied' not in st.session_state:
        st.session_state['filter_applied'] = False
        st.session_state['filtered_df'] = pd.DataFrame()

    configure_filters(df)

    if st.session_state['filter_applied']:

        display_data_3d_over_time(st.session_state['filtered_df'])


if __name__ == "__main__":
    main()
