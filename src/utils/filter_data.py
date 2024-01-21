import pandas as pd


class FilterData:
    def __init__(self, data: pd.DataFrame):
        self.data = self._verify_data(data)

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

    def filter_by_exchange(self, exchange: str) -> pd.DataFrame:
        return self.data[self.data['Exchange'] == exchange]


if __name__ == '__main__':
    print('This is filter_data.py')

    from src.utils.file_manager import FileManagerDynamic

    fms = FileManagerDynamic(ceiling_directory='30_TradingClub')
    df = fms.load_data(folder_name='data', file_name='exchange_concat.csv')
    print(df.head())
    fd = FilterData(df)
    print(fd.data.head())

