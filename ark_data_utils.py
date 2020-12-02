import requests
import os
from datetime import date
from data_analysis import DataAnalysis

DA = DataAnalysis("ark_holdings.db")
class ArkDataUtils:
    def __init__(self):
        self.urls = [
            "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv",
            "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_AUTONOMOUS_TECHNOLOGY_&_ROBOTICS_ETF_ARKQ_HOLDINGS.csv",
            "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_NEXT_GENERATION_INTERNET_ETF_ARKW_HOLDINGS.csv",
            "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_GENOMIC_REVOLUTION_MULTISECTOR_ETF_ARKG_HOLDINGS.csv",
            "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_FINTECH_INNOVATION_ETF_ARKF_HOLDINGS.csv",
            "https://ark-funds.com/wp-content/fundsiteliterature/csv/THE_3D_PRINTING_ETF_PRNT_HOLDINGS.csv",
            "https://ark-funds.com/wp-content/fundsiteliterature/csv/ARK_ISRAEL_INNOVATIVE_TECHNOLOGY_ETF_IZRL_HOLDINGS.csv",
        ]

    def get_ark_data(self):
        urls = self.urls
        newdir = self.make_folder()

        for url in urls:
            r = requests.get(url, allow_redirects=True)
            # Get file name
            fname = url.split("/")[-1]
            with open(newdir + fname, "wb") as csv:
                csv.write(r.content)
        return newdir

    def make_folder(self):
        today = date.today()
        newdir = os.getcwd() + "/data/" + str(today) + "/"
        if not (os.path.exists(newdir)):
            os.mkdir(newdir)
        return newdir


