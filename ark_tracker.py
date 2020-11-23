import requests
import os
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


def make_folder():
    today = date.today()
    newdir = os.getcwd() + "/data/" + str(today) + "/"
    if not (os.path.exists(newdir)):
        os.mkdir(newdir)
    return newdir


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
    send_email(newdir)

    print(str(datetime.now()) + " completed!!!")


def send_email(file_dir):

    files = glob.glob(file_dir + "*.csv")

    EMAIL_ADDRESS = os.environ.get("gmail_user")
    EMAIL_PASSWORD = os.environ.get("gmail_password")

    html = """\
            <html>
            <body>
                <p>Hi {name},<br><br>
                Here is your update for <strong><a href="https://ark-funds.com/investor-resources">ARK INVEST HOLDINGS</a></strong> as <b>{today}</b>.
                <br>
                <br>
                Good Luck,
                <br>
                <br>
                ARK Invest Tracker Team
                <br>
                ARK-Tracker-App V0.1
                </p>
            </body>
            </html>
            """
    
    receivers = {
        # 'Kwou' : 'kzhengnm@gmail.com',
        # 'Joe' : 'joe_yang999@yahoo.com',
        # 'Eugene': 'yuanlinsbu@gmail.com',
        'Lin': 'linzhengnm@gmail.com',
    }
    
    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)

        for name, email in receivers.items():
            msg = MIMEMultipart()
            msg["Subject"] = "ARK Tracker Daily Email ({0})".format(datetime.now().date())
            msg["From"] = EMAIL_ADDRESS
            msg["To"] = email
            msg.attach(MIMEText(html.format(name=name, today=datetime.now().date()), "html"))

            for file_path in files:
                file_name = file_path.split("/")[-1]
                with open(file_path, "rb") as attachment:
                    # Add file as application/octet-stream
                    # Email client can usually download this automatically as attachment
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                # Encode file in ASCII characters to send by email
                encoders.encode_base64(part)

                # Add header as key/value pair to attachment part
                part.add_header("Content-Disposition", f"attachment; filename= {file_name}")
                msg.attach(part)

            smtp.send_message(msg)
            del msg

        smtp.quit()

if __name__ == "__main__":
    scheduled_time = '21:00'
    schedule.every().monday.at(scheduled_time).do(get_data)
    schedule.every().tuesday.at(scheduled_time).do(get_data)
    schedule.every().wednesday.at(scheduled_time).do(get_data)
    schedule.every().thursday.at(scheduled_time).do(get_data)
    schedule.every().friday.at(scheduled_time).do(get_data)
    schedule.every().day.at(scheduled_time).do(get_data)

    while True:
        schedule.run_pending()
        time.sleep(1)
