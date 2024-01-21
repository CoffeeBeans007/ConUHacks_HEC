import pandas as pd
import streamlit as st
st.set_page_config(layout="wide")
from utils.filter_data import FilterData
from utils.file_manager import FileManagerDynamic
from utils.utils import display_dataframe_rows_over_time, processing_function



def get_number_input(filter_name, default_n=1):
    return st.number_input(f"Number of tickers for {filter_name}", min_value=1, value=default_n)


def get_text_input(filter_name, default_text=''):
    return st.text_input(f"Enter tickers for {filter_name}", value=default_text)


def configure_filters(df):
    st.title("Filter Configuration")

    exchange = st.selectbox("Select Exchange:", df['Exchange'].unique())
    df_exchange_filtered = FilterData(df).filter_by_exchange(exchange)

    selected_symbols = st.multiselect("Select Symbols:", df_exchange_filtered['Symbol'].unique(), key='symbols')
    if not isinstance(selected_symbols, list):
        selected_symbols = [selected_symbols]

    df_symbols_filtered = FilterData(df_exchange_filtered).filter_by_ticker_list(selected_symbols)

    filter_type = st.selectbox("Select filter type:", ["Top Tickers by MessageType",
                                                       "Top Tickers by Order Count",
                                                       "Filter by Event Type Sequence"],
                               index=2)
    df_filtered = pd.DataFrame()

    if filter_type == "Top Tickers by MessageType":
        n = get_number_input("Enter number of top tickers by message type:")
        df_filtered = FilterData(df_symbols_filtered).get_top_tickers_by_message_type(n)
    elif filter_type == "Top Tickers by Order Count":
        n = get_number_input("Enter number of top tickers by order count:")
        df_filtered = FilterData(df_symbols_filtered).get_top_tickers_by_order_count(n)
    elif filter_type == "Filter by Event Type Sequence":
        all_sequences = [
            ["NewOrderRequest", "NewOrderAcknowledged", "Trade", "CancelRequest", "CancelAcknowledged", "Cancelled"],
            ["NewOrderRequest", "NewOrderAcknowledged", "CancelRequest", "CancelAcknowledged", "Cancelled"],
            ["CancelRequest", "CancelAcknowledged", "Cancelled"],
            ["NewOrderRequest", "NewOrderAcknowledged"],
            ["NewOrderRequest", "Rejected"],
            ["Trade"]
        ]

        selected_sequences = st.multiselect("Select MessageType sequences:", all_sequences,
                                            format_func=lambda x: ' -> '.join(x))
        df_filtered = FilterData(df_symbols_filtered).filter_by_message_type_sequence(selected_sequences)

    if st.button("Apply Filter"):
        filter_data_instance = FilterData(df_filtered)
        st.session_state['filter_applied'] = True
        st.session_state['filtered_df'] = df_filtered
        st.write(df_filtered)


def main():

    st.title("QuantExplorerApplication")
    st.write("This is the main page")

    fms = FileManagerDynamic(ceiling_directory='30_TradingClub')
    df = fms.load_data(folder_name='data', file_name='exchange_concat.csv')

    if 'filter_applied' not in st.session_state:
        st.session_state['filter_applied'] = False
        st.session_state['filtered_df'] = pd.DataFrame()

    configure_filters(df)

    # if st.session_state['filter_applied']:
    #     display_dataframe_rows_over_time(st.session_state['filtered_df'])


if __name__ == "__main__":
    main()
