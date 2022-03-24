import requests as rq
import pandas as pd

# Get data about public transportation stops in switzerland
def get_sbb_data():
    response = rq.get("https://data.sbb.ch/api/v2/catalog/datasets/dienststellen-gemass-opentransportdataswiss/exports/json?limit=1&offset=0&timezone=UTC&apikey=8a89815df44a86552121399631d28f4472d3a150a9f1d6686ceb5c09")
    df_data = pd.DataFrame(response.json())
    print(df_data)
    df_data.to_csv("../data/sbb.csv")

#sOA3DSfAJ2rm7S4CCWIKMq8Z46BdNnM3:zU8G0ngJqaGjMkYf
payload = {'Authorization': 'Basic sOA3DSfAJ2rm7S4CCWIKMq8Z46BdNnM3:zU8G0ngJqaGjMkYf',
           'Cache-Control': 'no-cache',
           'Content-Length': '0',
           'Postman-Token': '24264e32-2de0-f1e3-f3f8-eab014bb6d76'}

response = rq.get("https://api.srgssr.ch/oauth/v1/accesstoken?grant_type=client_credentials", payload)
print(response.json())




