from os import curdir
from numpy.lib import twodim_base
import pandas as pd
from pandas.core.indexes.extension import make_wrapped_arith_op
from pretty_html_table import build_table
from db_utils import *
import glob
from pandas.tseries.offsets import BDay
from nyse_calendar import NYSECalendar
from datetime import datetime, date


class DataAnalysis:
    def __init__(self, db:str):
        self.db = ArkTrackDB(db)
        self.funds = ["ARKK", "ARKW", "ARKG", "ARKQ", "ARKF"]

    def view_all(self):
        db = self.db
        db.view_all()

    def top_ten_table(self, csv):
        df = pd.read_csv(csv)
        df.rename(str.upper, axis="columns", inplace=True)
        df.index = df.index + 1
        table = build_table(df.head(10), "grey_light")
        return table

    def save_to_db(self, file_dir):
        db = self.db
        file_list = glob.glob(file_dir + "/*.csv")
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
        # prev_tday = date.strftime(prev_tday, "%m/%d/%Y")
        return prev_tday

    def get_by_date(self, input_date):
        db = self.db
        # formatted_date = date.strftime(input_date, "%m/%d/%Y")
        formatted_date = '/'.join(map(str, [input_date.month, input_date.day, input_date.year]))
        non_trading_days = NYSECalendar().get_non_trading_days()
        if formatted_date not in non_trading_days:
            data = db.get_by_date(formatted_date)
            return data
        else:
            return None

    def get_top_ten_holdings(self, input_date, fund_name):
        df = self.get_by_date(input_date)
        holdings = df.loc[df["fund"] == fund_name].sort_values(by=['weight'], ascending=False)
        return holdings.head(10)

    def get_fund_holdings(self, input_date, fund_name):
        df = self.get_by_date(input_date)
        holdings = df.loc[df["fund"] == fund_name]
        return holdings

    def make_daily_top_ten_table(self, input_date, fund_name):
        curr_tday = date.strftime(input_date, "%m/%d/%Y")
        prev_tday = self.get_prev_trading_date(input_date)
        prev_tday_str = date.strftime(prev_tday, "%m/%d/%Y")
        top_ten_curr = self.get_top_ten_holdings(input_date, fund_name)
        top_ten_curr.reset_index(drop=True, inplace=True)
        top_ten_tickers = top_ten_curr['ticker'].to_list()

        data_prev = self.get_fund_holdings(prev_tday, fund_name)
        top_ten_prev = data_prev[data_prev['ticker'].isin(top_ten_tickers)].head(10)
        top_ten_prev['ticker'] = pd.Categorical(top_ten_prev['ticker'], top_ten_tickers)
        top_ten_prev.sort_values('ticker', inplace=True)
        top_ten_prev.reset_index(drop=True, inplace=True)

        column_names = [
            "Fund",
            "Company",
            "Ticker",
            "Shares on " + str(prev_tday_str),
            "Value on " + str(prev_tday_str),
            "Weight on " + str(prev_tday_str),
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
        df[column_names[10]] = ['${:,.2f}M'.format(x) for x in df[column_names[10]]/1000000]
        df[column_names[11]] = ((top_ten_curr['weight'] - top_ten_prev['weight'])/top_ten_prev['weight']).map('{:,.2%}'.format)
        df.reset_index(drop=True, inplace=True)

        daily_top_ten_table = build_table(df, "grey_light")
        return daily_top_ten_table

    def get_stock_diff(self, input_date, fund_name):
        prev_tday = self.get_prev_trading_date(input_date)
        curr_holdings = self.get_fund_holdings(input_date, fund_name)
        prev_holdings = self.get_fund_holdings(prev_tday, fund_name)
        curr_tickers = curr_holdings['ticker'].to_list()
        prev_tickers = prev_holdings['ticker'].to_list()

        result = {}
        curr_set = set(curr_tickers)
        prev_set = set(prev_tickers)
        result['fund'] = fund_name
        result['added'] = list(curr_set - prev_set)
        result['removed'] = list(prev_set - curr_set)
        return result

    def get_new_acquisitions(self, input_date):
        funds = self.funds
        new_acqs = []
        for fund in funds:
            result = self.get_stock_diff(input_date, fund)
            if len(result['added']):
                new_acqs.append(result)
        if len(new_acqs):
            return new_acqs
        else:
            return None

    def get_sell_offs(self, input_date):
        funds = self.funds
        sell_offs = []
        for fund in funds:
            result = self.get_stock_diff(input_date, fund)
            if len(result['removed']):
                sell_offs.append(result)
        if len(sell_offs):
            return sell_offs
        else:
            return None

    def make_new_acqs_table(self, input_date):
        new_acqs = self.get_new_acquisitions(input_date)
        df = pd.DataFrame(columns=['Fund', 'Ticker'])
        funds = []
        tickers = []
        if new_acqs is not None:
            for new_acq in new_acqs:
                funds.append(new_acq['fund'])
                tickers.append(', '.join(new_acq['added']))
        else:
            return 'No New Acquisition Today!<br>'

        df['Fund'] = funds
        df['Ticker'] = tickers
        acqs_table = build_table(df, "green_light")
        return acqs_table

    def make_sell_offs_table(self, input_date):
        sell_offs = self.get_sell_offs(input_date)
        df = pd.DataFrame(columns=['Fund', 'Ticker'])
        funds = []
        tickers = []
        if sell_offs is not None:
            for sell_off in sell_offs:
                funds.append(sell_off['fund'])
                tickers.append(', '.join(sell_off['added']))
        else:
            return 'No Stock Sell Off Today!<br>'

        df['Fund'] = funds
        df['Ticker'] = tickers
        sell_offs_table = build_table(df, "red_light")
        return sell_offs_table


# DA = DataAnalysis("ark_holdings.db")
# # # # DA.get_all()
# # funds = ['ARKK', 'ARKQ', 'ARKW', 'ARKG']
# today = date.today().replace(month=11, day=30)
# x = DA.make_new_acqs_table(today)

# print(x)