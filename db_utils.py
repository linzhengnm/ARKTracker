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
            weight real NOT NULL
            )"""

        my_cursor.execute(sql)
        con.commit()
        con.close()


    def insert_row(self, row:tuple):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        my_cursor.execute("INSERT INTO arkdata VALUES (?, ?, ?, ? ,?, ?, ?, ?)", row)
        con.commit()
        con.close()

    def insert_rows(self, rows):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        my_cursor.executemany("INSERT INTO arkdata VALUES (?, ?, ?, ? ,?, ?, ?, ?)",rows)
        con.commit()
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
        con.close
        return result

    def drop_table(self):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        my_cursor.execute("DROP TABLE arkdata")
        con.commit()
        con.close()

    

