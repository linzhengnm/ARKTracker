import pandas as pd
from pretty_html_table import build_table
from db_utils import *
import glob

def top_ten_table(csv):
    df = pd.read_csv(csv)
    df.rename(str.upper, axis='columns', inplace=True)
    df.index = df.index+1
    table = build_table(df.head(10), 'grey_light')
    return table

def save_to_db(file_dir):
    file_list = glob.glob(file_dir+'*.csv') 
    for csv_file in file_list:
        with open(csv_file, 'rb') as fin:
            df = pd.read_csv(csv_file)
            rows = [row for row in df[:-3].itertuples(index=False)]
            insert_rows(rows)


