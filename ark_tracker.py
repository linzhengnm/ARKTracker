import requests
import os
import time
import schedule
from datetime import date, datetime

def make_folder():
    today = date.today()
    newdir = os.getcwd() + "/data/" + str(today) + "/"
    if not (os.path.exists(newdir)):
        os.mkdir(newdir)
    return(newdir)


def get_data():
    urls = [
        "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/fundsiteliterature/csv/THE_3D_PRINTING_ETF_PRNT_HOLDINGS.csv",
        "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_ISRAEL_INNOVATIVE_TECHNOLOGY_ETF_IZRL_HOLDINGS.csv",
    ]

    newdir = make_folder()
    for url in urls:
        r = requests.get(url, allow_redirects=True)
        # Get file name
        fname = url.split("/")[-1]

        with open(newdir + fname, "wb") as csv:
            csv.write(r.content)

    print(str(datetime.now()) + ' completed!!!')

scheduled_time = '21:00'
schedule.every().monday.at(scheduled_time).do(get_data)
schedule.every().tuesday.at(scheduled_time).do(get_data)
schedule.every().wednesday.at(scheduled_time).do(get_data)
schedule.every().thursday.at(scheduled_time).do(get_data)
schedule.every().friday.at(scheduled_time).do(get_data)

while True:
    schedule.run_pending()
    time.sleep(1)
