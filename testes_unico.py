import pandas as pd
import os

raw_folder = "data/raw/"
anos = range(2017, 2026)


df = pd.read_csv(os.path.join(raw_folder, "acidentes2017.csv"), delimiter=';', encoding='latin1')

print(df.info())

""" staging_folder = "data/staging/"
df = pd.read_parquet(os.path.join(staging_folder, "acidentes2017.parquet"))
print(df.info()) """