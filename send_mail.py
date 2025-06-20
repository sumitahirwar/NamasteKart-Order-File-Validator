import smtplib
from datetime import date

def send_mail (subject , body) :
    try :
        server = smtplib.SMTP('smtp.gmail.com',587)
        server.starttls()
        server.login('sumit02feb@gmail.com','key_here')
        message = f"Subject: {subject}\n\n{body}"
        server.sendmail('sumit02feb@gmail.com','sumitahirwar2@gmail.com',message)
        print('mail sent')
    except Exception as e :
        print("Error occured during mail send")
