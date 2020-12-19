import os
import sys
import time
import schedule
from datetime import date, datetime
from data_analysis import DataAnalysis
from ark_data_utils import ArkDataUtils
from email_utils import EmailUtils

def run_ark_bot():
    input_date = date.today().replace(day=18)
    ADU = ArkDataUtils(input_date) 
    DA = DataAnalysis("ark_holdings.db")
    
    receivers = {
            # 'Kwou' : 'kzhengnm@gmail.com',
            "Joe": "joe_yang999@yahoo.com",
            "Eugene": "yuanjinglin88@gmail.com",
            "Lin": "linzhengnm@gmail.com",
        }
    EMAIL = EmailUtils(input_date, receivers)

    data = ADU.get_ark_data()
    DA.save_to_db(data)
    EMAIL.send_email()

    print('Job Completed!!!')


if __name__ == "__main__":
    run_ark_bot()
    
    # scheduled_time = "19:30"

    # schedule.every().monday.at(scheduled_time).do(run_ark_bot)
    # schedule.every().tuesday.at(scheduled_time).do(run_ark_bot)
    # schedule.every().wednesday.at(scheduled_time).do(run_ark_bot)
    # schedule.every().thursday.at(scheduled_time).do(run_ark_bot)
    # schedule.every().friday.at(scheduled_time).do(run_ark_bot)

    # while True:
    #     schedule.run_pending()
    #     print(datetime.now())
    #     time.sleep(1)