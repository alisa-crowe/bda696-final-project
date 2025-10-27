import pandas as pd
import numpy as np  

file_paths = [
    "./reddit_players_batch01_fast.csv",
    "./reddit_players_batch01.csv",
    "./reddit_players_batch02_fast.csv",
    "./reddit_players_batch03_big.csv",
    "./reddit_players_batch04_big.csv",
    "./reddit_players_batch05_big.csv",
    "./reddit_players_batch06_big.csv"
]

df_list = [pd.read_csv(fp) for fp in file_paths]
df_combined = pd.concat(df_list, ignore_index=True)
df_combined.to_csv("./player_data.csv", index=False)