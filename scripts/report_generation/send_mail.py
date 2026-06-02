import os
import smtplib
from email.message import EmailMessage
from datetime import datetime
# from dotenv import load_dotenv

# -------------------- ENV --------------------
# load_dotenv()

EMAIL_USER = os.environ["EMAIL_USER"] 
EMAIL_PASS = os.environ["EMAIL_PASS"]    
EMAIL_TO   = os.environ["EMAIL_TO"]         

ATTACHMENT_PATH = "./data/Aggregated_Hours.xlsx"

# -------------------- BUILD EMAIL --------------------
def send_email():
    print(f"Starting email send → {datetime.now()}")

    msg = EmailMessage()
    msg["Subject"] = "3 Months FWD Looking Resource Forecast"
    msg["From"] = EMAIL_USER
    msg["To"] = EMAIL_TO.split(",")

    msg.set_content(
        "Hi,\n\n"
        "Please find attached the latest aggregated team hours report that looks at the next 3 months.\n\n"
        "Regards,\n"
        "Your Automation Pipeline"
    )

    # -------------------- ATTACH FILE --------------------
    if not os.path.exists(ATTACHMENT_PATH):
        raise FileNotFoundError(f"Attachment not found: {ATTACHMENT_PATH}")

    with open(ATTACHMENT_PATH, "rb") as f:
        file_data = f.read()
        file_name = os.path.basename(ATTACHMENT_PATH)

        msg.add_attachment(
            file_data,
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=file_name
        )

    # -------------------- SEND --------------------
    with smtplib.SMTP("smtp.office365.com", 587) as smtp:
        smtp.starttls()
        smtp.login(EMAIL_USER, EMAIL_PASS)
        smtp.send_message(msg)

    print(f"Email sent successfully → {datetime.now()}")


# -------------------- MAIN --------------------
if __name__ == "__main__":
    send_email()
