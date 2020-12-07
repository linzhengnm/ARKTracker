import os
import sys
import time
import schedule
from datetime import date, datetime
from data_analysis import DataAnalysis
from ark_data_utils import ArkDataUtils
from email_utils import EmailUtils

def run_ark_bot():
    ADU = ArkDataUtils() 
    DA = DataAnalysis("ark_holdings.db")
    EMAIL = EmailUtils()

    data = ADU.get_ark_data()
    DA.save_to_db(data)
    EMAIL.send_email(data)

    print('Job Completed!!!')


if __name__ == "__main__":
    run_ark_bot()
    
    # scheduled_time = "19:00"

    # schedule.every().monday.at(scheduled_time).do(run_ark_bot)
    # schedule.every().tuesday.at(scheduled_time).do(run_ark_bot)
    # schedule.every().wednesday.at(scheduled_time).do(run_ark_bot)
    # schedule.every().thursday.at(scheduled_time).do(run_ark_bot)
    # schedule.every().friday.at(scheduled_time).do(run_ark_bot)

    # while True:
    #     schedule.run_pending()
    #     print(datetime.now())
    #     time.sleep(1)