from os import curdir
from numpy.core.fromnumeric import sort
from numpy.lib import twodim_base
from numpy.lib.arraysetops import unique
import pandas as pd
from pandas.core.algorithms import diff, rank
from pandas.core.indexes.extension import make_wrapped_arith_op
from pandas_datareader.compat import lmap
from pretty_html_table import build_table
from yfinance import ticker
from db_utils import *
import glob
from pandas.tseries.offsets import BDay
from nyse_calendar import NYSECalendar
from datetime import datetime, date
import json
import yfinance as yf



class DataAnalysis:
    def __init__(self, db: str):
        self.db = ArkTrackDB(db)
        self.funds = ["ARKK", "ARKW", "ARKG", "ARKQ", "ARKF"]

    def view_all(self):
        db = self.db
        db.view_all()

    # def top_ten_table(self, csv):
    #     df = pd.read_csv(csv)
    #     df.rename(str.upper, axis="columns", inplace=True)
    #     df.index = df.index + 1
    #     table = build_table(df.head(10), "grey_light")
    #     return table

    def _update_tickers(self, tickers, df):
        diff_tickers = set(df['ticker']) - set(tickers.keys())
        print('call for update!!!')

        if len(diff_tickers):
            new_tics = {}
            for tic in diff_tickers:
                new_tics[tic] = tic
            tickers.update(new_tics)
            with open('stock_tickers.json', 'w') as f:
                json.dump(tickers, f, sort_keys=True)

    def _load_tickers(self):
        with open ("stock_tickers.json") as json_file:
            tickers = json.load(json_file)
        return tickers

    def save_to_db(self, file_dir):
        db = self.db
        tickers = self._load_tickers()
        file_list = glob.glob(file_dir + "/*.csv")
        date_str = file_dir.split('/')[-1]
        input_date = datetime.strptime(date_str, '%Y-%m-%d')
        existed_data = self.get_by_date(input_date)

        if existed_data is None:
            for csv_file in file_list:
                df = pd.read_csv(csv_file)
                df = df[:-3]
                df = df.fillna('None')
                self._update_tickers(tickers, df)
                tickers = self._load_tickers()
                df['ticker'] = [tickers[key] for key in df['ticker'].to_list()]
                start_date = datetime.strptime(df['date'][0], '%m/%d/%Y').strftime('%Y-%m-%d')
                stock_data = yf.download(df['ticker'].to_list(), start=start_date, period='1d')
                df['close'] = df['ticker'].apply(lambda x: 0 if x=='None' else stock_data.Close[x][0])

                # for tic in df['ticker'].to_list():
                #     tics.append(tickers[tic])
                rows = [row for row in df.itertuples(index=False, name=None)]
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
        formatted_date = "/".join(
            map(str, [input_date.month, input_date.day, input_date.year])
        )
        non_trading_days = NYSECalendar().get_non_trading_days()
        if formatted_date not in non_trading_days:
            df = db.get_by_date(formatted_date)
            if df is not None:
                df = df.loc[df["fund"].str.contains("ARK")]
                df = df.loc[~df['ticker'].str.contains('None')]
            return df
        else:
            return None

    def get_top_ten_holdings(self, input_date, fund_name):
        df = self.get_by_date(input_date)
        holdings = df.loc[df["fund"] == fund_name].sort_values(
            by=["weight"], ascending=False
        )
        return holdings.head(10)

    def get_fund_holdings(self, input_date, fund_name):
        df = self.get_by_date(input_date)
        holdings = df.loc[df["fund"] == fund_name]
        return holdings

    def make_daily_top_ten_table(self, input_date, fund_name):
        curr_tday = date.strftime(input_date, "%m/%d/%Y")
        prev_tday = self.get_prev_trading_date(input_date)
        prev_tday_str = date.strftime(prev_tday, "%m/%d/%Y")
        top_curr = self.get_top_ten_holdings(input_date, fund_name)
        top_curr.reset_index(drop=True, inplace=True)
        top_tickers = top_curr["ticker"].to_list()

        data_prev = self.get_fund_holdings(prev_tday, fund_name)
        top_prev = data_prev[data_prev["ticker"].isin(top_tickers)].head(10)
        top_prev["ticker"] = pd.Categorical(top_prev["ticker"], top_tickers)
        top_prev.sort_values("ticker", inplace=True)
        top_prev.reset_index(drop=True, inplace=True)

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
            "Weight Difference",
        ]

        df = pd.DataFrame(columns=column_names)
        df[column_names[0]] = top_curr["fund"]
        df[column_names[1]] = top_curr["company"]
        df[column_names[2]] = top_curr["ticker"]
        df[column_names[3]] = top_prev["shares"].map("{:,.0f}".format)
        df[column_names[4]] = [
            "${:,.2f}M".format(x) for x in top_prev["value"] / 1000000
        ]
        df[column_names[5]] = top_prev["weight"].map("{:,.2f}%".format)
        df[column_names[6]] = top_curr["shares"].map("{:,.0f}".format)
        df[column_names[7]] = [
            "${:,.2f}M".format(x) for x in top_curr["value"] / 1000000
        ]
        df[column_names[8]] = top_curr["weight"].map("{:,.2f}%".format)
        df[column_names[9]] = (top_curr["shares"] - top_prev["shares"]).map(
            "{:,.0f}".format
        )
        df[column_names[10]] = top_curr["value"] - top_prev["value"]
        df[column_names[10]] = [
            "${:,.2f}M".format(x) for x in df[column_names[10]] / 1000000
        ]
        df[column_names[11]] = (
            (top_curr["weight"] - top_prev["weight"]) / top_prev["weight"]
        ).map("{:,.2%}".format)
        df.reset_index(drop=True, inplace=True)

        daily_top_holdings_table = build_table(df, "grey_light")
        return daily_top_holdings_table

    def get_stock_diff(self, input_date, fund_name):
        prev_tday = self.get_prev_trading_date(input_date)
        curr_holdings = self.get_fund_holdings(input_date, fund_name)
        prev_holdings = self.get_fund_holdings(prev_tday, fund_name)
        curr_tickers = curr_holdings["ticker"].to_list()
        prev_tickers = prev_holdings["ticker"].to_list()

        result = {}
        curr_set = set(curr_tickers)
        prev_set = set(prev_tickers)
        result["fund"] = fund_name
        result["added"] = list(curr_set - prev_set)
        result["removed"] = list(prev_set - curr_set)
        return result

    def get_new_acquisitions(self, input_date):
        funds = self.funds
        new_acqs = []
        for fund in funds:
            result = self.get_stock_diff(input_date, fund)
            if len(result["added"]):
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
            if len(result["removed"]):
                sell_offs.append(result)
        if len(sell_offs):
            return sell_offs
        else:
            return None

    def make_new_acqs_table(self, input_date):
        new_acqs = self.get_new_acquisitions(input_date)
        df = pd.DataFrame(columns=["Fund", "Ticker"])
        funds = []
        tickers = []
        if new_acqs is not None:
            for new_acq in new_acqs:
                funds.append(new_acq["fund"])
                tickers.append(", ".join(new_acq["added"]))
        else:
            return "No New Acquisition Today!<br>"

        df["Fund"] = funds
        df["Ticker"] = tickers
        acqs_table = build_table(df, "green_light")
        return acqs_table

    def make_sell_offs_table(self, input_date):
        sell_offs = self.get_sell_offs(input_date)
        df = pd.DataFrame(columns=["Fund", "Ticker"])
        funds = []
        tickers = []
        if sell_offs is not None:
            for sell_off in sell_offs:
                funds.append(sell_off["fund"])
                tickers.append(", ".join(sell_off["removed"]))
        else:
            return "No Stock Sell Off Today!<br>"

        df["Fund"] = funds
        df["Ticker"] = tickers
        sell_offs_table = build_table(df, "red_light")
        return sell_offs_table

    def get_curr_total_top_holdings(self, input_date, top_num):
        ark_funds = self.get_by_date(input_date)
        # holdings = holdings[~holdings['ticker'].str.contains(pat = '[0-9]', regex = True)]
        # ark_funds = holdings.loc[holdings["fund"].str.contains("ARK")]
        # ark_funds = ark_funds.loc[~ark_funds['ticker'].str.contains('None')]

        top_holdings = (
            ark_funds.groupby(["ticker", "company","close"])
            .sum()
            .sort_values(by=["value"], ascending=False)
            .head(top_num)
            .reset_index()
        )

        df = pd.DataFrame()
        df["rank"] = top_holdings.index + 1
        df["ticker"] = top_holdings["ticker"]
        df["company"] = top_holdings["company"]
        df["shares"] = top_holdings["shares"]
        df["value"] = top_holdings["value"]
        df['close'] = top_holdings['close']     
        
        return df

    def get_prev_total_top_holdings(self, input_date, tickers):
        ark_funds = self.get_by_date(input_date)
        # holdings = holdings[~holdings['ticker'].str.contains(pat = '[0-9]', regex = True)]
        # ark_funds = holdings.loc[holdings["fund"].str.contains("ARK")]
        # ark_funds = ark_funds.loc[~ark_funds['ticker'].str.contains('None')]


        group_sums = ark_funds.groupby(["ticker", "company","close"]).sum()
        sorted_sums = group_sums.sort_values(by="value", ascending=False).reset_index()

        df = pd.DataFrame()
        df["rank"] = sorted_sums.index + 1
        df["ticker"] = sorted_sums["ticker"]
        df["company"] = sorted_sums["company"]
        df["shares"] = sorted_sums["shares"]
        df["value"] = sorted_sums["value"]
        df["close"] = sorted_sums["close"]
        top_holdings = df.loc[df["ticker"].isin(tickers)]
        top_holdings["ticker"] = pd.Categorical(top_holdings["ticker"], tickers)
        top_holdings.sort_values(by="ticker", inplace=True)
        top_holdings.reset_index(drop=True, inplace=True)
        # top_holdings = top_holdings[
        #     ~top_holdings.ticker.str.contains(pat="[0-9]", regex=True)
        # ]
        return top_holdings

    # def get_prev_ranks(self, input_date, top_ten_tickers):
    #     holdings = self.get_by_date(input_date)
    #     ark_funds = holdings.loc[holdings['fund'].str.contains('ARK')]
    #     group_sums = ark_funds.groupby(['ticker', 'company']).sum()
    #     sorted_sums = group_sums.sort_values(by="value", ascending=False).reset_index()
    #     df = pd.DataFrame(columns=['rank', 'ticker'])
    #     df['rank'] = sorted_sums.index + 1
    #     df['ticker'] = sorted_sums['ticker']
    #     top_ten = df.loc[df['ticker'].isin(top_ten_tickers)]
    #     top_ten['ticker'] = pd.Categorical(top_ten['ticker'], top_ten_tickers)
    #     top_ten.sort_values(by='ticker', inplace=True)
    #     top_ten.reset_index(drop=True, inplace=True)

    #     print(top_ten)
    def get_daily_total_top_holdings(self, input_date, top_num):
        curr_tday_str = date.strftime(input_date, "%m/%d/%Y")
        prev_tday = self.get_prev_trading_date(input_date)
        prev_tday_str = date.strftime(prev_tday, "%m/%d/%Y")
        top_curr = self.get_curr_total_top_holdings(input_date, top_num)
        top_tickers = top_curr["ticker"].to_list()
        top_prev = self.get_prev_total_top_holdings(prev_tday, top_tickers)
        df_merged = top_curr.merge(top_prev, on=["ticker", 'company'], how='left')

        column_names = [
            "Rank",
            "Movement",
            "Company",
            "Ticker",
            "Shares On " + str(prev_tday_str),
            "Value On " + str(prev_tday_str),
            "Price On " + str(prev_tday_str),
            "Shares On " + str(curr_tday_str),
            "Value On " + str(curr_tday_str),
            "Price On " + str(curr_tday_str),
            "Shares Difference",
            "Value Difference",
        ]

        rank_diff_str_list = []
        ranks = pd.DataFrame()
        rank_diffs = (df_merged["rank_y"] - df_merged["rank_x"]).to_list()

        for rank_diff in rank_diffs:
            if rank_diff > 0:
                rank_diff_str_list.append("\u25B2 " + "%.0f" % abs(rank_diff))
            elif rank_diff < 0:
                rank_diff_str_list.append("\u25BC " + "%.0f" % abs(rank_diff))
            else:
                rank_diff_str_list.append("\u27F3")

        ranks["diff"] = rank_diffs
        ranks["diff_str"] = rank_diff_str_list

        df = pd.DataFrame(columns=column_names)
        df[column_names[0]] = df_merged["rank_x"]
        df[column_names[1]] = ranks["diff_str"]
        df[column_names[2]] = df_merged["company"]
        df[column_names[3]] = df_merged["ticker"]
        df[column_names[4]] = df_merged["shares_y"]
        df[column_names[5]] = df_merged["value_y"]
        df[column_names[6]] = df_merged["close_y"]
        df[column_names[7]] = df_merged["shares_x"]
        df[column_names[8]] = df_merged["value_x"]
        df[column_names[9]] = df_merged["close_x"]
        df[column_names[10]] = df_merged["shares_x"] - df_merged["shares_y"]
        df[column_names[11]] = df_merged["value_x"] - df_merged["value_y"]


        df_tickers = df.Ticker.to_list()
        df_ref = self.get_by_date(input_date)
        fund_list = []
        for tic in df_tickers:
            result = df_ref.loc[df_ref['ticker'] == tic]
            fund_list.append(', '.join(result['fund'].to_list()))
        df['Funds'] = fund_list

        df['Direction'] = df['Shares Difference'].apply(lambda x : "Buy" if x > 0 else ( "Sell" if x < 0  else "Hold") )
        df.loc[df.iloc[:, 4].isnull(), 'Direction'] = 'New Position'
        df.loc[df.iloc[:, 7] == 0, 'Direction'] = 'Sell Off'

        # df = df[~df.Ticker.str.contains(pat="[0-9]", regex=True)]

        return df

    def format_table(self, df):
        df.iloc[:, 4] = df.iloc[:, 4].map("{:,.0f}".format)
        df.iloc[:, 5] = ["${:,.2f}M".format(x) for x in df.iloc[:, 5] / 1000000]
        df.iloc[:, 6] = ["${:,.2f}".format(x) for x in df.iloc[:, 6]]
        df.iloc[:, 7] = df.iloc[:, 7].map("{:,.0f}".format)
        df.iloc[:, 8] = ["${:,.2f}M".format(x) for x in df.iloc[:, 8] / 1000000]
        df.iloc[:, 9] = ["${:,.2f}".format(x) for x in df.iloc[:, 9]]
        df.iloc[:, 10] = df.iloc[:, 10].map("{:,.0f}".format)
        df.iloc[:, 11] = ["${:,.2f}M".format(x) for x in df.iloc[:, 11] / 1000000]
        return df

    def make_daily_total_top_holdings_table(self, input_date, top_num):
        df = self.get_daily_total_top_holdings(input_date, top_num)
        df = self.format_table(df)
        daily_total_top_holdings_table = build_table(df, "grey_light")
        return daily_total_top_holdings_table

    def get_daily_active_trades(self, input_date, top_num):
        df = self.get_daily_total_top_holdings(input_date, 500)
        # Sort by shares diff and value diff
        # Using temp column "a" and "b" for absolute value sorting
        df["a"] = df["Shares Difference"].abs() * df.iloc[:, 9]
        # df["b"] = df["Value Difference"].abs()
        df = df.sort_values(by="a", ascending=False)
        df = df.drop(columns=["a"])
        return df.head(top_num)

    def get_daily_active_buys(self, input_date, top_num):
        df = self.get_daily_total_top_holdings(input_date, 500)
        # Sort by shares diff and value diff
        # Using temp column "a" and "b" for absolute value sorting
        sorted_df = df.sort_values(
            by=["Shares Difference", "Value Difference"], ascending=[False, False]
        )
        return sorted_df.head(top_num)

    def make_daily_active_trades_table(self, input_date, top_num):
        df = self.get_daily_active_trades(input_date, top_num)
        df = self.format_table(df)
        daily_active_table = build_table(df, "grey_light")
        return daily_active_table

    def make_daily_active_buys_table(self, input_date, top_num):
        df = self.get_daily_active_buys(input_date, top_num)
        df = self.format_table(df)
        daily_active_buys_table = build_table(df, "grey_light")
        return daily_active_buys_table

    def get_nyse_tickers(self, input_date):
        df = self.get_daily_total_top_holdings(input_date, 500)
        df = df[~df.Ticker.str.contains(pat="[0-9]", regex=True)]
        tickers = df.Ticker.to_list()
        new_tickers = []

        for ticker in tickers:
            t = ticker.split(" ")
            new_tickers.append(t[0])

        df.Ticker = new_tickers


# DA = DataAnalysis("ark_holdings.db")
# # # # # # # # DA.get_all()
# # # # # # funds = ['ARKK', 'ARKQ', 'ARKW', 'ARKG']
# today = date.today().replace(month=12, day=8)
# x = DA.get_by_date(today)
# print(x)
# # yesterday = date.today().replace(month=12, day=7)
# # x = DA.get_curr_total_top_holdings(today)
# # print(x)
# # top_tickers = x['ticker'].to_list()
# # # print(top_tickers)
# # y = DA.get_prev_total_top_holdings(yesterday, top_tickers)
# # print(y)
# # # DA.get_prev_ranks(yesterday, top_tickers)
# # DA.make_daily_total_top_holdings_table(today)
# x = DA.get_daily_active_trades(today, 15)
# print(x)
# x = DA.get_nyse_tickers(today)
