import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime

SMTP_HOST  = "10.36.66.46"
SMTP_PORT  = 25
FROM_EMAIL = "ttrs@mobicom.mn"
TO_EMAILS  = ["barsbold@mobicom.mn", "bilegt@mobicom.mn", "tugsbayar@mobicom.mn", "bumdari.b@mobicom.mn", "uuganbayar.ulzii@mobicom.mn"]
CC_EMAILS  = []

ZTE_FILE    = "output_analysis_zte.xlsx"
HUAWEI_FILE = "output_analysis_huawei.xlsx"


def build_email(today_str, attachments):
    subject = f"Сайтын баттери анализ тайлан — {today_str}"
    body    = f"""Сайн байна уу,

{today_str}-ны өдрийн сайтын баттернуудын анализ тайланг хавсаргав.

- Баттерийн ерөнхий байдал
- Drop percent
- Status & Forecast

Хүндэтгэсэн,
TTRS"""

    msg = MIMEMultipart()
    msg['From']    = FROM_EMAIL
    msg['To']      = ', '.join(TO_EMAILS)
    if CC_EMAILS:
        msg['CC']  = ', '.join(CC_EMAILS)
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain', 'utf-8'))

    for filepath in attachments:
        filename = os.path.basename(filepath)
        with open(filepath, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
            msg.attach(part)

    return msg


def main():
    today_str   = datetime.today().strftime('%Y-%m-%d')
    attachments = []

    for filepath in [ZTE_FILE, HUAWEI_FILE]:
        if os.path.exists(filepath):
            attachments.append(filepath)
            print(f"Attachment: {filepath}")
        else:
            print(f"{filepath} not found, skip")

    if not attachments:
        return

    msg = build_email(today_str, attachments)
    all_recipients = TO_EMAILS + CC_EMAILS

    try:
        server = smtplib.SMTP()
        server.connect(SMTP_HOST, SMTP_PORT)
        server.ehlo()
        server.sendmail(FROM_EMAIL, all_recipients, msg.as_string())
        server.quit()
        print(f"Email successful send → {', '.join(TO_EMAILS)}")
    except smtplib.SMTPConnectError as e:
        print(f"Connection error: {e}")
    except smtplib.SMTPException as e:
        print(f"SMTP error: {e}")
    except Exception as e:
        print(f"Error: {type(e).__name__}: {e}")


if __name__ == '__main__':
    main()
