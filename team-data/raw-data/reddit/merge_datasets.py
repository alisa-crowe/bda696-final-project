import pandas as pd
import glob
import os

def merge_datasets():
    folder = '/Users/alisacrowe/Desktop/BDA 696/final-project/sentiment-data'
    csv_files = glob.glob(os.path.join(folder, "reddit_batch*.csv"))

    df = []
    for file in sorted(csv_files):
        print(f"Reading {file}...")
        df.append(pd.read_csv(file))
    
    merged_df = pd.concat(df, ignore_index=True)

    output_path = os.path.join(folder, "reddit_data.csv")
    merged_df.to_csv(output_path, index=False)

if __name__ == "__main__":
    merge_datasets()