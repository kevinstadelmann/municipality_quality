import pandas as pd

df_municipal_stats = pd.read_excel("../data/je-d-21.03.01_Kennzahlen_Gemeinde_2021.xlsx",
                                   usecols=[1,2,4,6,8,9,10,11,12,13,14,15,16,22,31,33,34,35,36,37,38,39,40,41],
                                   skipfooter=16)

new_header = df_municipal_stats.iloc[4] #grab the row for the header
df_municipal_stats = df_municipal_stats[9:] #take the data less the header row
df_municipal_stats.columns = new_header.values #set the header row as the df header
df_municipal_stats = df_municipal_stats.reset_index(drop=True)

print(pd.DataFrame(df_municipal_stats))