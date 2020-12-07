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
    def send_email(self, file_dir):
        files = glob.glob(file_dir + "/*.csv")
        EMAIL_ADDRESS = os.environ.get("ark_tracker_email")
        EMAIL_PASSWORD = os.environ.get("ark_tracker_password")
        html = """\
                <html>
            
                <body>
                    <p>Hi {name},<br><br>
                    AS <b>{today}</b>
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

                    <b>TOP 10 HOLDINGS </b>
                    <br>
                    {table0}
                    <br>
                    {table1}
                    <br>
                    {table2}
                    <br>
                    {table3}
                    <br>
                    {table4}
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
        tables = []
        funds = ["ARKK", "ARKW", "ARKG", "ARKQ", "ARKF"]
        today = date.today().replace(day=4)

        new_acqs_table = DA.make_new_acqs_table(today)
        sell_offs_table = DA.make_sell_offs_table(today)

        for fund in funds:
            tables.append(DA.make_daily_top_ten_table(today, fund))
        # for file in files:
        #     tables.append(DA.top_ten_table(file))

        receivers = {
            # 'Kwou' : 'kzhengnm@gmail.com',
            # "Joe": "joe_yang999@yahoo.com",
            # "Eugene": "yuanjinglin88@gmail.com",
            "Lin": "linzhengnm@gmail.com",
        }

        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)

            for name, email in receivers.items():
                msg = MIMEMultipart()
                msg["Subject"] = "ARK Tracker Daily Email ({0})".format(
                    datetime.now().date()
                )
                msg["From"] = EMAIL_ADDRESS
                msg["To"] = email
                msg.attach(
                    MIMEText(
                        html.format(
                            new_acqs_table=new_acqs_table,
                            sell_offs_table=sell_offs_table,
                            name=name,
                            today=datetime.now().date(),
                            table0=tables[0],
                            table1=tables[1],
                            table2=tables[2],
                            table3=tables[3],
                            table4=tables[4],
                        ),
                        "html",
                    )
                )

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
                    part.add_header(
                        "Content-Disposition", f"attachment; filename= {file_name}"
                    )
                    msg.attach(part)

                smtp.send_message(msg)
                del msg

            smtp.quit()