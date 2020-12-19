import os
from datetime import date, datetime
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import glob
from data_analysis import DataAnalysis

DA = DataAnalysis("ark_holdings.db")

class EmailUtils():
    def __init__(self, input_date, receivers):
        self.input_date = input_date
        self.receivers = receivers
        
    def send_email(self):
        # files = glob.glob(file_dir + "/*.csv")
        EMAIL_ADDRESS = os.environ.get("ark_tracker_email")
        EMAIL_PASSWORD = os.environ.get("ark_tracker_password")
        html = """\
                <html>
            
                <body>
                    <p>Hi {name},<br><br>
                    AS <b>{input_date}</b>
                    <br>
                    Here is your update for <strong><a href="https://ark-funds.com/investor-resources">ARK INVEST</a></strong>.
                    <br>
                    <br>
                    <strong>Today's New Acquisitions:</strong>
                    <br>
                    {new_acqs_table}
                    <br>
                    <strong>Today's Sell-Offs:</strong>
                    <br>
                    {sell_offs_table}
                    <br>

                    <b>TOP HOLDINGS </b>
                    <br>
                    {top_holdings_table}
                    <br>

                    <b>ACTIVE TRADES </b>
                    <br>
                    {daily_active_table}
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
        # tables = []
        # funds = ["ARKK", "ARKW", "ARKG", "ARKQ", "ARKF"]
        input_date = self.input_date
        new_acqs_table = DA.make_new_acqs_table(input_date)
        sell_offs_table = DA.make_sell_offs_table(input_date)
        top_holdings_table = DA.make_daily_total_top_holdings_table(input_date, 500)
        daily_active_table = DA.make_daily_active_trades_table(input_date, 20)
        receivers = self.receivers
        # daily_active_buys_table = DA.make_daily_active_buys_table(input_date, 15)

        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)

            for name, email in receivers.items():
                msg = MIMEMultipart()
                msg["Subject"] = "ARK Tracker Daily Email ({0})".format(
                    input_date
                )
                msg["From"] = EMAIL_ADDRESS
                msg["To"] = email
                msg.attach(
                    MIMEText(
                        html.format(
                            new_acqs_table=new_acqs_table,
                            sell_offs_table=sell_offs_table,
                            name=name,
                            input_date=input_date,
                            top_holdings_table = top_holdings_table,
                            daily_active_table = daily_active_table
                            # daily_active_buys_table = daily_active_buys_table
                        ),
                        "html",
                    )
                )

                # for file_path in files:
                #     file_name = file_path.split("/")[-1]
                #     with open(file_path, "rb") as attachment:
                #         # Add file as application/octet-stream
                #         # Email client can usually download this automatically as attachment
                #         part = MIMEBase("application", "octet-stream")
                #         part.set_payload(attachment.read())
                #     # Encode file in ASCII characters to send by email
                #     encoders.encode_base64(part)

                #     # Add header as key/value pair to attachment part
                #     part.add_header(
                #         "Content-Disposition", f"attachment; filename= {file_name}"
                #     )
                #     msg.attach(part)

                smtp.send_message(msg)
                del msg

            smtp.quit()