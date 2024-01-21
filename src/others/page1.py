import webbrowser
import pandas as pd
import datetime
import time
import streamlit as st

from utils.file_manager import FileManagerDynamic

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def graph_dataframe_rows_over_time(df: pd.DataFrame, duration_minutes: int = 4):
    if df.empty:
        st.write("The DataFrame is empty.")
        return

    graph_placeholder = st.empty()
    start_time = df['TimeStamp'].min()
    end_time = df['TimeStamp'].max()

    total_seconds = duration_minutes * 60
    total_rows = int((end_time - start_time).total_seconds())
    sleep_time = total_seconds / total_rows

    current_time = start_time
    fig = create_initial_figure(df)

    while current_time < end_time:
        next_time = current_time + datetime.timedelta(seconds=1)
        fig = update_figure(df, current_time, next_time, fig)
        graph_placeholder.plotly_chart(fig, use_container_width=True)

        time.sleep(sleep_time)
        current_time = next_time


def create_initial_figure(df):
    symbols = df['Symbol'].unique()
    message_types = df['MessageType'].unique()
    symbol_to_num = {symbol: i for i, symbol in enumerate(symbols)}
    message_type_to_num = {message_type: i for i, message_type in enumerate(message_types)}

    fig = go.Figure()
    fig.update_layout(
        title='MessageType per Symbol Over Time',
        scene=dict(
            xaxis=dict(title='Time (seconds from start)'),
            yaxis=dict(title='MessageType', tickvals=list(message_type_to_num.values()),
                       ticktext=list(message_type_to_num.keys())),
            zaxis=dict(title='Symbol', tickvals=list(symbol_to_num.values()), ticktext=list(symbol_to_num.keys()))
        ),
        legend=dict(title='MessageType'),
        showlegend=True
    )
    return fig


def update_figure(df, current_time, next_time, fig):
    filtered_df = df[(df['TimeStamp'] >= current_time) & (df['TimeStamp'] < next_time)]

    if not filtered_df.empty:
        start_time = df['TimeStamp'].min()
        filtered_df['TimeFraction'] = (filtered_df['TimeStamp'] - start_time).dt.total_seconds()
        filtered_df['Order'] = filtered_df['TimeStampEpoch'] - filtered_df['TimeStampEpoch'].min()

        symbols = df['Symbol'].unique()
        message_types = df['MessageType'].unique()
        symbol_to_num = {symbol: i for i, symbol in enumerate(symbols)}
        message_type_to_num = {message_type: i for i, message_type in enumerate(message_types)}

        plotly_markers = ['circle', 'square', 'diamond', 'cross', 'x', 'triangle-up', 'star']

        for message_type in message_types:
            df_type = filtered_df[filtered_df['MessageType'] == message_type]
            if not df_type.empty:
                x = df_type['TimeFraction']
                y = [message_type_to_num[mt] for mt in df_type['MessageType']]
                z = [symbol_to_num[symbol] for symbol in df_type['Symbol']]
                color = df_type['Order']

                marker_symbol = plotly_markers[message_types.tolist().index(message_type)]

                trace = go.Scatter3d(
                    x=x, y=y, z=z,
                    mode='markers',
                    marker=dict(
                        size=5,
                        symbol=marker_symbol,
                        color=color,
                        colorscale='Viridis',
                        opacity=0.8
                    ),
                    name=message_type
                )
                fig.add_trace(trace)

        fig.update_layout(
            title='MessageType per Symbol Over Time',
            scene=dict(
                xaxis=dict(title='Time (seconds from start)'),
                yaxis=dict(title='Symbol', tickvals=list(symbol_to_num.values()), ticktext=list(symbol_to_num.keys())),
                zaxis=dict(title='MessageType', tickvals=list(message_type_to_num.values()), ticktext=list(message_type_to_num.keys()))
            ),
            legend=dict(title='MessageType'),
            showlegend=True
        )

        # Ajout d'une légende pour l'ordre d'apparition
        colorbar_trace = go.Scatter3d(
            x=[None], y=[None], z=[None],
            mode='markers',
            marker=dict(
                colorscale='Viridis',
                colorbar=dict(title='Order of Messages'),
                showscale=True
            ),
            hoverinfo='none',
            showlegend=False
        )
        fig.add_trace(colorbar_trace)

    return fig


def save_and_show_figure_in_html(fig):
    html_file = 'graph.html'  # Remplacer par le chemin souhaité
    fig.write_html(html_file)
    webbrowser.open(html_file, new=2)  # Ouvre le graphique dans un nouvel onglet du navigateur


def app():
    df = read_data_csv(file_name="exchange_concat.csv", folder_name="data")
    n_random_tickers = find_n_random_tickers(df, n=2)
    df_filtered = filter_dataframe_by_tickers(df, n_random_tickers)

    print(df_filtered.head())


if __name__ == "__main__":
    app()


