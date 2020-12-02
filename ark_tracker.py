from pandas.io.pytables import DataIndexableCol
import requests
import os
import sys
import time
import schedule
from datetime import date, datetime
import smtplib
from email.message import EmailMessage
from email import encoders
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import glob
from data_analysis import DataAnalysis
from ark_data_utils import ArkDataUtils
from email_utils import EmailUtils

def run_ark_bot():
    ADU = ArkDataUtils() 
    DA = DataAnalysis("ark_holdings.db")
    # EMAIL = EmailUtils()

    data = ADU.get_ark_data()
    DA.save_to_db(data)
    # EMAIL.send_email(data)


if __name__ == "__main__":
    run_ark_bot()
    
    # scheduled_time = "19:00"

    # schedule.every().monday.at(scheduled_time).do(get_data)
    # schedule.every().tuesday.at(scheduled_time).do(get_data)
    # schedule.every().wednesday.at(scheduled_time).do(get_data)
    # schedule.every().thursday.at(scheduled_time).do(get_data)
    # schedule.every().friday.at(scheduled_time).do(get_data)

    # while True:
    #     schedule.run_pending()
    #     print(datetime.now())
    #     time.sleep(1)