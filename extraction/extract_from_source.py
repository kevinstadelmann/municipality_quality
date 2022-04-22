import requests
import pandas as pd
from io import StringIO
from municipality_quality.extraction import extract_multiple_weather_station

def download_file(ip_url: str, ip_encoding: str, ip_file_type: str, ip_sheet_name: str, ip_skiprows: int):
    # Function to download different types of files
    # Job with multiple files
    if ip_url == 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_*':
        data = extract_multiple_weather_station.download_multiple_weather_station()
        f = StringIO()
        data.to_csv(f, index=False, sep=';')
        f.seek(0)
        f = f.read()
    # xlsx source files need to be converted to csv
    elif ip_file_type == 'xlsx':
        data = pd.read_excel(ip_url, sheet_name=ip_sheet_name, skiprows=ip_skiprows)
        f = StringIO()
        data.to_csv(f, index=False)
        f.seek(0)
        f = f.read()
    # Single json or csv files
    else:
        data = requests.get(ip_url).content
        data = str(data, ip_encoding)
        #f = StringIO()
        f = data

    return f
