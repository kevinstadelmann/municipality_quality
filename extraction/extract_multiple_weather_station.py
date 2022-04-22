import pandas as pd

def download_multiple_weather_station():
    # Download meta data file of weather station
    url = 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/liste-download-nbcn-d.csv'

    stationMain = pd.read_csv(url, encoding='ISO-8859-1', sep=';')
    stationMain = stationMain.dropna(subset=['Station'])

    li = []

    # df = df.reset_index()  # make sure indexes pair with number of rows
    for index, row in stationMain.iterrows():
        # Save files
        # Previous years
        # url1 = row['URL Previous years (verified data)']
        # df_station = pd.read_csv(url1, encoding='ISO-8859-1', sep=';', index_col=None, header=0)
        # li.append(df_station)

        # Current year
        url2 = row['URL Current year']
        df_station = pd.read_csv(url2, encoding='ISO-8859-1', sep=';', index_col=None, header=0)
        li.append(df_station)

    stationStats = pd.concat(li, axis=0, ignore_index=True)

    return stationStats
