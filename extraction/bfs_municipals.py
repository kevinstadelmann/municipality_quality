import pandas as pd
import requests

url = "https://dam-api.bfs.admin.ch/hub/api/dam/assets/7226419/master"

def create_df_zipcode(url):
    r = requests.get(url)
    data = r.content
    df_zipcode = pd.read_excel(data, sheet_name="PLZ6",
                           usecols=[0, 1, 2, 4, 5, 6],
                           header=None,
                           names=["zipcode_4", "zipcode_6",
                                  "zipcode_name", "canton_abbrev",
                                  "municip_nr", "municip_name"],
                           skiprows=1)
    return df_zipcode
