import sqlite3
import os
import glob
import csv
import pandas as pd

def create_db():
    con = sqlite3.connect('ark_data.db')
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


def insert_row(row:tuple):
    con = sqlite3.connect('ark_data.db')
    my_cursor = con.cursor()
    my_cursor.execute("INSERT INTO arkdata VALUES (?, ?, ?, ? ,?, ?, ?, ?)", row)
    con.commit()
    con.close()

def insert_rows(rows):
    con = sqlite3.connect('ark_data.db')
    my_cursor = con.cursor()
    my_cursor.executemany("INSERT INTO arkdata VALUES (?, ?, ?, ? ,?, ?, ?, ?)",rows)
    con.commit()
    con.close()

def view_all():
    con = sqlite3.connect('ark_data.db')
    my_cursor = con.cursor()
    my_cursor.execute("SELECT * FROM arkdata")
    result = my_cursor.fetchall()
    for row in result:
        print(row)
    con.close()

def drop_table():
    con = sqlite3.connect('ark_data.db')
    my_cursor = con.cursor()
    my_cursor.execute("DROP TABLE arkdata")
    con.commit()
    con.close()

    

