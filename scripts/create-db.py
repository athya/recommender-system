import pandas as pd
import sqlite3

df = pd.read_csv('./scripts/movie.csv')
conn = sqlite3.connect('movie.db')
df.to_sql('movie', conn, if_exists='replace', index=False)
conn.close()