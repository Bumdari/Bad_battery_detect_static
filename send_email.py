import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime

SMTP_SERVER = 'smtp.office365.com'
SMTP_PORT   = 587

SENDER_EMAIL    = 'bumdari1019@gmail.com'       
SENDER_PASSWORD = 'ejozsxjyqiwqsrpq'               

TO_EMAILS  = [
    'barsbold@mobicom.mn',
]
CC_EMAILS  = []  
BCC_EMAILS = []   

ATTACHMENT_PATH = 'output_analysis_da2_2.xlsx'           

today_str = datetime.today().strftime('%Y-%m-%d')
SUBJECT = f'Сайтуудын баттерийн тайлан — {today_str}'
BODY    = f"""\
Сайн байна уу,

{today_str}-ны өдрийн сайтуудын баттери тайланг хавсаргав.

Тайлангийн агуулга:
  • Баттерийн ерөнхий байдал
  • Drop percent
  • Status & Forecast — цаашдын таамаглал

Асуух зүйл байвал холбоо барина уу TTST-Автоматжуулалтын баг.

Хүндэтгэсэн,
Автомат мэдэгдэл
"""

def send_email():
    if not os.path.exists(ATTACHMENT_PATH):
        print(f"Файл олдсонгүй: {ATTACHMENT_PATH}")
        return

    msg = MIMEMultipart()
    msg['From']    = SENDER_EMAIL
    msg['To']      = ', '.join(TO_EMAILS)
    if CC_EMAILS:
        msg['CC']  = ', '.join(CC_EMAILS)
    msg['Subject'] = SUBJECT
    msg.attach(MIMEText(BODY, 'plain', 'utf-8'))

    with open(ATTACHMENT_PATH, 'rb') as f:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename="{os.path.basename(ATTACHMENT_PATH)}"'
        )
        msg.attach(part)

    all_recipients = TO_EMAILS + CC_EMAILS + BCC_EMAILS

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.sendmail(SENDER_EMAIL, all_recipients, msg.as_string())
        print(f"✓ Имэйл амжилттай илгээгдлээ → {', '.join(TO_EMAILS)}")
        if CC_EMAILS:
            print(f"  CC: {', '.join(CC_EMAILS)}")
        if BCC_EMAILS:
            print(f"  BCC: {', '.join(BCC_EMAILS)}")
    except smtplib.SMTPAuthenticationError:
        print("Нэвтрэх нэр эсвэл нууц үг буруу байна")
    except smtplib.SMTPException as e:
        print(f"SMTP алдаа: {e}")
    except Exception as e:
        print(f"Алдаа: {e}")


if __name__ == '__main__':
    send_email()