import pandas as pd
# from find_patterns import FindPatterns
from utils.find_patterns import FindPatterns


class FilterData:
    def __init__(self, data: pd.DataFrame):
        self.data = self._verify_data(data)
        self.find_patterns = FindPatterns(self.data)

    def _verify_data(self, data: pd.DataFrame) -> pd.DataFrame:
        data['TimeStamp'] = pd.to_datetime(data['TimeStamp'], errors='coerce')
        data['TimeStampEpoch'] = pd.to_numeric(data['TimeStampEpoch'], errors='coerce', downcast='integer')
        data.sort_values('TimeStamp', inplace=True)
        return data

    def get_top_tickers_by_message_type(self, n: int) -> pd.DataFrame:
        unique_message_types = self.data.groupby('Symbol')['MessageType'].nunique()
        top_tickers = unique_message_types.nlargest(n).index.tolist()
        return self.data[self.data['Symbol'].isin(top_tickers)]

    def filter_by_ticker_list(self, tickers: list) -> pd.DataFrame:
        return self.data[self.data['Symbol'].isin(tickers)]

    def get_top_tickers_by_order_count(self, n: int) -> pd.DataFrame:
        order_counts = self.data.groupby('Symbol')['OrderID'].nunique()
        top_tickers = order_counts.nlargest(n).index.tolist()
        return self.data[self.data['Symbol'].isin(top_tickers)]

    def filter_by_exchanges(self, exchanges: list) -> pd.DataFrame:
        return self.data[self.data['Exchange'].isin(exchanges)]

    def filter_by_message_type_sequence(self, selected_sequences: list[list[str]]) -> pd.DataFrame:
        # Get the grouped DataFrame with OrderID and MessageType sequences
        grouped_sequences = self.find_patterns._group_by_order_id()

        # Convert the list of sequences into a set for faster lookup
        sequences_set = {' -> '.join(seq) for seq in selected_sequences}

        # Find all OrderIDs that have a sequence contained in our set of selected sequences
        matching_order_ids = grouped_sequences[
            grouped_sequences['Sequence'].isin(sequences_set)
        ]['OrderID'].unique()

        # Filter the DataFrame by the OrderIDs from matching sequences
        filtered_df = self.data[self.data['OrderID'].isin(matching_order_ids)]

        return filtered_df.drop_duplicates()


if __name__ == '__main__':
    print('This is filter_data.py')

    from src.utils.file_manager import FileManagerDynamic

    fms = FileManagerDynamic(ceiling_directory='30_TradingClub')
    df = fms.load_data(folder_name='data', file_name='exchange_concat.csv')
    print(df.head())
    fd = FilterData(df)
    print(fd.data.head())
    sequences_to_filter = [['Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade', 'Trade']
    ]

    # Filter by the selected sequences
    order_id_filtered = fd.filter_by_message_type_sequence(selected_sequences=sequences_to_filter)
    print(order_id_filtered)
