import pandas as pd

df_municipal = pd.read_excel("../data/municipals.xlsx", header=None, sheet_name='PLZ4')

print(df_municipal)