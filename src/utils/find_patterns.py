import pandas as pd
from collections import defaultdict


class FindPatterns:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def _group_by_order_id(self):
        grouped = self.df.groupby('OrderID')['MessageType'].apply(lambda x: ' -> '.join(x)).reset_index(name='Sequence')
        return grouped

    def _find_patterns(self, grouped):
        pattern_counts = grouped['Sequence'].value_counts().to_dict()
        return pattern_counts

    def _map_patterns(self, pattern_counts):
        pattern_mapping = {v: f'pattern_{i + 1}' for i, v in enumerate(pattern_counts.keys())}
        return pattern_mapping

    def find_and_count_patterns(self):
        grouped = self._group_by_order_id()
        pattern_counts = self._find_patterns(grouped)
        pattern_mapping = self._map_patterns(pattern_counts)

        # Create a dictionary with 'pattern_x' as keys and sequence as values
        pattern_full_counts = {pattern_mapping[sequence]: count for sequence, count in pattern_counts.items()}
        return pattern_full_counts, pattern_mapping

    def replace_pattern_keys(self, pattern_full_counts, pattern_mapping):
        # Reverse the pattern_mapping dictionary
        reverse_mapping = {v: k for k, v in pattern_mapping.items()}
        # Replace 'pattern_x' keys with the actual sequence
        patterns_with_arrows = {reverse_mapping[k]: v for k, v in pattern_full_counts.items()}
        # sort by value
        patterns_sorted = {k: v for k, v in sorted(patterns_with_arrows.items(), key=lambda item: item[1], reverse=True)}
        return patterns_sorted


if __name__ == '__main__':
    print('This is find_patterns.py')

    from src.utils.file_manager import FileManagerDynamic

    fms = FileManagerDynamic(ceiling_directory='30_TradingClub')
    df = fms.load_data(folder_name='data', file_name='exchange_concat.csv')
    print(df.head())

    fp = FindPatterns(df)
    pattern_full_counts, pattern_mapping = fp.find_and_count_patterns()
    print(pattern_full_counts)
    print(pattern_mapping)
    patterns = fp.replace_pattern_keys(pattern_full_counts=pattern_full_counts, pattern_mapping=pattern_mapping)
    print(patterns)


