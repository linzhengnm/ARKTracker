import sqlite3
import os
import glob
import csv
import pandas as pd

class ArkTrackDB():
    def __init__(self, db):
        self.db = db

    def create_table(self):
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
            close real,
            unique (date, fund, company, shares, value)
            )"""

        my_cursor.execute(sql)
        con.commit()
        print('Your table has been created!!!')
        con.close()

    def insert_row(self, row:tuple):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        try:
            my_cursor.execute("INSERT INTO arkdata VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?)", row)
            con.commit()
        except:
            pass
        con.close()

    def insert_rows(self, rows:list):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        try:
            my_cursor.executemany("INSERT INTO arkdata VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?)",rows)
            con.commit()
            my_cursor.close()
            print('inserted successfully!!!')

        except:
            print("insert failed!!!")
            pass
        con.close()

    def view_all(self, table_name):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        my_cursor.execute("SELECT * FROM {}".format(table_name))
        result = my_cursor.fetchall()
        for row in result:
            print(row)
        con.close()

    def get_by_date(self, date):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        try:
            my_cursor.execute("SELECT * FROM arkdata WHERE date=?", (date, ))
            result = my_cursor.fetchall()
            df = pd.DataFrame(result)
            df.columns = [x[0] for x in my_cursor.description]
            con.close
            return df
        except:
            return None

    def drop_table(self, table_name):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        my_cursor.execute("DROP TABLE {}".format(table_name))
        con.commit()
        con.close()

    def add_new_column(self, column_name):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        addColumn = "ALTER TABLE arkdata ADD COLUMN {} real".format(column_name)
        my_cursor.execute(addColumn)
        con.close

    def change_data(self):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()

        sql = """
        CREATE TABLE arkdata_1 (
            date text NOT NULL,
            fund text NOT NULL,
            company text NOT NULL,
            ticker text,
            cusip text NOT NULL,
            shares real NOT NULL,
            value real NOT NULL,
            weight real NOT NULL,
            close real,
            unique (date, fund, company, shares, value)
            )
        """

        sql2 = """
        INSERT INTO arkdata_1 (date, fund, company, ticker, cusip, shares, value, weight) SELECT * FROM arkdata
        """
        sql3 = """
        ALTER TABLE arkdata_1 RENAME TO arkdata;
        """
        sql4 = """
        DELETE FROM arkdata WHERE date = '12/18/2020' 
        """
        # my_cursor.execute(sql)
        # my_cursor.execute(sql2)
        my_cursor.execute(sql4)
        con.commit()
    #     print('Your column has been dropped!!!')
        con.close()
        
    def insert_rows_new(self, rows):
        con = sqlite3.connect(self.db)
        my_cursor = con.cursor()
        try:
            my_cursor.executemany("INSERT INTO arkdata_1 VALUES (?, ?, ?, ? ,?, ?, ?, ?, ?)",rows)
            con.commit()
            my_cursor.close()
            print('inserted successfully!!!')
        except:
            print("insert failed!!!")
            pass
        con.close()

    


# db = ArkTrackDB('ark_holdings.db')
# # # # # db.drop_table('arkdata')
# db.change_data()
# # # # # # # # # # rows = [('11/23/2020', 'PRNT', 'SLM SOLUTIONS GROUP AG', 'AM3D.DE', 'BMHTHK2', '286827.0', '4991543.62', '6.35', '14.700000')]
# # # # # # # # # # db.insert_rows_new(rows)
# # # # # # # db.drop_table('arkdata_1')
# db.view_all('arkdata')



    





