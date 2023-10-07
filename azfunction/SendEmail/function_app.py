import azure.functions as func
import logging
import json
from email.message import EmailMessage
import smtplib
from email.mime.text import MIMEText



app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def send_email(mail):
    '''
    send messages to email
    '''
    message = MIMEText(mail['messages'], "html", "utf-8")
    message["From"] = mail['sender']
    message["To"] = mail['recipient']
    message["Subject"] = mail['subject']
    sender_auth = mail['mail_auth']

    smtp = smtplib.SMTP_SSL("smtp.163.com", port=994)
    smtp.login(mail['sender'], sender_auth)
    smtp.sendmail(mail['sender'], mail['recipient'], message.as_string())
    smtp.close()
    logging.info(f'ok')


@app.route(route="SendEmail")
def SendEmail(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body_bytes = req.get_body()

    req_body = req_body_bytes.decode("utf-8")

    try:
        send_email_json = json.loads(req_body)

    except:
        logging.info(f'{req_body} can not be parsed to json')
    # logging.info(req_body )
    try:
        send_email(send_email_json)
        return func.HttpResponse(
            f"ok",
            status_code=200,
            )
    except:
        logging.info(f'Cannot send email')
        return func.HttpResponse(
            f"not ok",
            status_code=401,
        )