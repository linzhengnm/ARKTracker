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
        files = glob.glob(file_dir + "*.csv")
        EMAIL_ADDRESS = os.environ.get("ark_tracker_email")
        EMAIL_PASSWORD = os.environ.get("ark_tracker_password")
        html = """\
                <html>
            
                <body>
                    <p>Hi {name},<br><br>
                    Here is your update for <strong><a href="https://ark-funds.com/investor-resources">ARK INVEST</a></strong></b>.
                    <br>
                    <br>
                    <b>TOP 10 HOLDINGS AS <b>{today}<b>
                    <br>
                    {table1}
                    <br>
                    {table2}
                    <br>
                    {table3}
                    <br>
                    {table4}
                    <br>
                    {table5}
                    <br>
                    {table6}
                    <br>
                    {table7}
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
        funds = ["PRNT", "IZRL", "ARKK", "ARKW", "ARKG", "ARKQ", "ARKF"]
        today = date.today()

        for fund in funds:
            tables.append(DA.make_daily_top_ten_table(today, fund))
        # for file in files:
        #     tables.append(DA.top_ten_table(file))

        receivers = {
            # 'Kwou' : 'kzhengnm@gmail.com',
            "Joe": "joe_yang999@yahoo.com",
            "Eugene": "yuanjinglin88@gmail.com",
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
                            name=name,
                            today=datetime.now().date(),
                            table1=tables[0],
                            table2=tables[1],
                            table3=tables[2],
                            table4=tables[3],
                            table5=tables[4],
                            table6=tables[5],
                            table7=tables[6],
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