import pandas as pd
import streamlit as st
from utils.filter_data import FilterData
from utils.file_manager import FileManagerDynamic


def get_number_input(filter_name, default_n=5):
    return st.number_input(f"Number of tickers for {filter_name}", min_value=1, value=default_n)


def get_text_input(filter_name, default_text=''):
    return st.text_input(f"Enter tickers for {filter_name}", value=default_text)


def configure_filters(df):
    st.title("Filter Configuration")

    exchange = st.selectbox("Select Exchange:", df['Exchange'].unique())
    df_exchange_filtered = FilterData(df).filter_by_exchange(exchange)

    filter_type = st.selectbox("Select filter type:", ["Top Tickers by MessageType",
                                                       "Filter by Ticker List",
                                                       "Top Tickers by Order Count"])

    df_filtered = pd.DataFrame()

    if filter_type == "Top Tickers by MessageType":
        n = get_number_input("Enter number of top tickers by message type:")
        df_filtered = FilterData(df_exchange_filtered).get_top_tickers_by_message_type(n)
    elif filter_type == "Filter by Ticker List":
        available_tickers = df_exchange_filtered['Symbol'].unique()
        selected_tickers = st.multiselect("Select tickers:", available_tickers)
        df_filtered = FilterData(df_exchange_filtered).filter_by_ticker_list(selected_tickers)
    elif filter_type == "Top Tickers by Order Count":
        n = get_number_input("Enter number of top tickers by order count:")
        df_filtered = FilterData(df_exchange_filtered).get_top_tickers_by_order_count(n)

    if st.button("Apply Filter"):
        st.write(df_filtered)


def main():
    st.title("QuantExplorerApplication")
    st.write("This is the main page")

    fms = FileManagerDynamic(ceiling_directory='30_TradingClub')

    df = fms.load_data(folder_name='data', file_name='exchange_concat.csv')
    configure_filters(df)


if __name__ == "__main__":
    main()
