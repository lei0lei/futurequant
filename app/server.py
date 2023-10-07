'''
邮件发送
'''
from send import *
import requests

if __name__ == '__main__':
    data = main()
    sender = "13091171683@163.com"
    sender_auth = "OUBDVKZHNFSBZUXY"
    recipient = "lei.lei.fan.meng@gmail.com"
    url = 'https://sendemailfutureinfo.azurewebsites.net/api/SendEmail'
    messages = format_messages_to_html(data)
    subject = '交易推荐'
    # print(messages)
    send_json = {}
    send_json['sender'] = sender
    send_json['recipient'] = recipient
    send_json['subject'] = subject
    send_json['mail_auth'] = sender_auth
    send_json['messages'] = messages
    r = requests.post(url, json=send_json)
    # send_email(messages, sender, sender_auth, recipient)