import pandas as pd

df = pd.read_csv("online_retail_II.csv", encoding="unicode_escape")
print(df.info())
