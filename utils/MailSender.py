import smtplib
from email.mime import multipart
from email.mime import text
from email import message
from email.mime.text import MIMEText


class MailSender:
    def __init__(self, app_pass):
        self.smtp_server = "smtp.gmail.com"
        self.smtp_port = 587
        self.username = "fujiken36@gmail.com"
        self.app_pass = app_pass
        self.to_address = "fujiken36@gmail.com"

    def send_mail_to_fujiken36(self, subject, body):
        message = MIMEText(body)
        message["Subject"] = subject
        message["From"] = self.username
        message["To"] = self.to_address

        smtp = smtplib.SMTP(self.smtp_server, self.smtp_port)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(self.username, self.app_pass)
        smtp.send_message(message)
        smtp.quit()


    def send_mail_with_file_to_fujiken36(self, subject, body, file_path):
        msg = multipart.MIMEMultipart()
        msg.attach(text.MIMEText(body, 'plain'))

        with open(file_path, 'r') as f:
            attachment = text.MIMEText(f.read(), 'plain')
            attachment.add_header(
                'Content-Disposition', 'attachment', filename=file_path
            )
            msg.attach(attachment)

        msg["Subject"] = subject
        msg["From"] = self.username
        msg["To"] = self.to_address

        smtp = smtplib.SMTP(self.smtp_server, self.smtp_port)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(self.username, self.app_pass)
        smtp.sendmail(self.username, [self.to_address], msg.as_string())
        smtp.quit()
