import pandas as pd
from pretty_html_table import build_table

def top_ten_table(csv):
    df = pd.read_csv(csv)
    df.rename(str.upper, axis='columns', inplace=True)
    df.index = df.index+1
    table = build_table(df.head(10), 'grey_light')
    return table


