import sqlite3
import os
import glob
import csv
import pandas as pd

class ArkTrackDB():
    def __init__(self, db):
        self.db = db

    def create_db(self):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()

        sql = """
        CREATE TABLE arkdata (
            date text NOT NULL,
            fund text NOT NULL,
            company text NOT NULL,
            ticker text,
            cusip text NOT NULL,
            shares real NOT NULL,
            value real NOT NULL,
            weight real NOT NULL,
            unique (date, fund, ticker)
            )"""

        my_cursor.execute(sql)
        con.commit()
        print('Your Database has been created!!!')
        con.close()

    def insert_row(self, row:tuple):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        try:
            my_cursor.execute("INSERT INTO arkdata VALUES (?, ?, ?, ? ,?, ?, ?, ?)", row)
            con.commit()
        except:
            pass
        con.close()

    def insert_rows(self, rows:tuple):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        try:
            my_cursor.executemany("INSERT INTO arkdata VALUES (?, ?, ?, ? ,?, ?, ?, ?)",rows)
            con.commit()
        except:
            pass
        con.close()

    def view_all(self):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        my_cursor.execute("SELECT * FROM arkdata")
        result = my_cursor.fetchall()
        for row in result:
            print(row)
        con.close()

    def get_by_date(self, date):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        my_cursor.execute("SELECT * FROM arkdata WHERE date=?", (date, ))
        result = my_cursor.fetchall()
        df = pd.DataFrame(result)
        df.columns = [x[0] for x in my_cursor.description]
        con.close
        return df

    def drop_table(self):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        my_cursor.execute("DROP TABLE arkdata")
        con.commit()
        con.close()

