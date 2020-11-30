from os import curdir
from numpy.lib import twodim_base
import pandas as pd
from pretty_html_table import build_table
from db_utils import *
import glob
from pandas.tseries.offsets import BDay
from nyse_calendar import NYSECalendar
from datetime import datetime, date

class DataAnalysis():
    def __init__(self):
        self.db = ArkTrackDB('ark_data.db')

    def top_ten_table(self,csv):
        df = pd.read_csv(csv)
        df.rename(str.upper, axis='columns', inplace=True)
        df.index = df.index+1
        table = build_table(df.head(10), 'grey_light')
        return table

    def save_to_db(self, file_dir):
        db = self.db
        file_list = glob.glob(file_dir+'*.csv') 
        for csv_file in file_list:
            with open(csv_file, 'rb') as fin:
                df = pd.read_csv(csv_file)
                rows = [row for row in df[:-3].itertuples(index=False)]
                db.insert_rows(rows)

    def get_prev_trading_date(self, curr_tday):
        # today = datetime.today()
        prev_tday = (curr_tday - BDay(1)).date()
        non_trading_days = NYSECalendar().get_non_trading_days()
        if prev_tday in non_trading_days:
            prev_tday = (prev_tday - BDay(1)).date() 
        return prev_tday

    def get_by_date(self, input_date):
        db = self.db
        formatted_date = date.strftime(input_date, "%m/%d/%Y")
        print(formatted_date)
        data = db.get_by_date(formatted_date)
        return data

# db = ArkTrackDB('ark_data.db')
# # db.view_all()
# x = db.get_by_date('11/25/2020')
# print(x)