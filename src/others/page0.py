im

def app():
    st.title("Page 0")
    st.write("This is page 0")

    fms = FileManagerDynamic(ceiling_directory='30_TradingClub')

    df = fms.load_data(folder_name='data', file_name='exchange_concat.csv')
    configure_filters(df)



