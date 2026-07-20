import pandas as pd
import os

raw_folder = "data//"
anos = range(2017, 2026)

df = pd.read_csv("data/01_bronze/acidentes2017.csv", encoding='latin1', sep=';')
print(df.dtypes)

print('='*50)
