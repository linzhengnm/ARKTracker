from os import curdir
from numpy.lib import twodim_base
import pandas as pd
from pretty_html_table import build_table
from db_utils import *
import glob
from pandas.tseries.offsets import BDay
from nyse_calendar import NYSECalendar
from datetime import datetime, date


class DataAnalysis:
    def __init__(self):
        self.db = ArkTrackDB("ark_data.db")

    def top_ten_table(self, csv):
        df = pd.read_csv(csv)
        df.rename(str.upper, axis="columns", inplace=True)
        df.index = df.index + 1
        table = build_table(df.head(10), "grey_light")
        return table

    def save_to_db(self, file_dir):
        db = self.db
        file_list = glob.glob(file_dir + "*.csv")
        for csv_file in file_list:
            with open(csv_file, "rb") as fin:
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
        non_trading_days = NYSECalendar().get_non_trading_days()
        if formatted_date not in non_trading_days:
            data = db.get_by_date(formatted_date)
            return data
        else:
            return None

    def get_top_ten_holdings(self, input_date, fund_name):
        df = self.get_by_date(input_date)
        holdings = df.loc[df["fund"] == fund_name]
        return holdings.head(10)

    def get_fund_holdings(self, input_date, fund_name):
        df = self.get_by_date(input_date)
        holdings = df.loc[df["fund"] == fund_name]
        return holdings

    def make_daily_top_ten_table(self, input_date, fund_name):
        curr_tday = date.strftime(input_date, "%m/%d/%Y")
        prev_tday = self.get_prev_trading_date(input_date)
        top_ten_curr = self.get_top_ten_holdings(input_date, fund_name)
        # top_ten_curr.reset_index(drop=True, inplace=True)
        top_ten_tickers = top_ten_curr['ticker'].to_list()

        data_prev = self.get_fund_holdings(prev_tday, fund_name)
        top_ten_prev = data_prev[data_prev['ticker'].isin(top_ten_tickers)].head(10)
        top_ten_prev['ticker'] = pd.Categorical(top_ten_prev['ticker'], top_ten_tickers)
        top_ten_prev.sort_values('ticker', inplace=True)
        # top_ten_prev.reset_index(drop=True, inplace=True)

        column_names = [
            "Fund",
            "Company",
            "Ticker",
            "Shares on " + str(prev_tday),
            "Value on " + str(prev_tday),
            "Weight on " + str(prev_tday),
            "Shares on " + str(curr_tday),
            "Value on " + str(curr_tday),
            "Weight on " + str(curr_tday),
            "Shares Difference",
            "Value Difference",
            "Weight Difference"
        ]

        df = pd.DataFrame(columns=column_names)
        df[column_names[0]] = top_ten_curr['fund']
        df[column_names[1]] = top_ten_curr['company']
        df[column_names[2]] = top_ten_curr['ticker']
        df[column_names[3]] = top_ten_prev['shares'].map('{:,.0f}'.format)
        df[column_names[4]] = ['${:,.2f}M'.format(x) for x in top_ten_prev['value']/1000000]
        df[column_names[5]] = top_ten_prev['weight'].map('{:,.2f}%'.format)
        df[column_names[6]] = top_ten_curr['shares'].map('{:,.0f}'.format)
        df[column_names[7]] = ['${:,.2f}M'.format(x) for x in top_ten_curr['value']/1000000]
        df[column_names[8]] = top_ten_curr['weight'].map('{:,.2f}%'.format)
        df[column_names[9]] = (top_ten_curr['shares'] - top_ten_prev['shares']).map('{:,.0f}'.format)
        df[column_names[10]] = top_ten_curr['value'] - top_ten_prev['value']
        df[column_names[11]] = ['${:,.2f}M'.format(x) for x in df[column_names[11]]/1000000]
        df[column_names[11]] = (top_ten_curr['weight'] - top_ten_prev['weight']).map('{:,.2f}%'.format)
        df.reset_index(drop=True, inplace=True)

        daily_top_ten_table = build_table(df, "grey_light")
        return daily_top_ten_table

    def get_all(self):
        db = self.db
        db.view_all()


DA = DataAnalysis()
DA.get_all()
# today = datetime.today().replace(month=11, day=30)
# DA.make_daily_top_ten_table(today, "ARKW")
# # x = DA.get_by_date(today)
# print(x)
# db = ArkTrackDB('ark_data.db')
# db.view_all()
# x = db.get_by_date('11/25/2020')
# print(x)
