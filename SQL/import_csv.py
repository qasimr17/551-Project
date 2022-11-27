import pandas as pd 
import numpy as np 

df = pd.read_csv('cars.csv')
df["sort_index"] = np.arange(len(df))

df1, df2 = df.iloc[:103], df.iloc[103:]
df1.to_csv('p1.csv', index=False)
df2.to_csv('p2.csv', index=False)
# df3.to_csv('p14.csv', index=False)