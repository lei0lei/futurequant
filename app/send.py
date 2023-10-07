'''
邮件发送
'''
from algo import main

from email.message import EmailMessage
import smtplib
from email.mime.text import MIMEText


def send_email(messages, sender, sender_auth, recipient):
    '''
    send messages to email
    '''
    message = MIMEText(messages, "html", "utf-8")
    message["From"] = sender
    message["To"] = recipient
    message["Subject"] = "Your Personal Future choices"

    smtp = smtplib.SMTP_SSL("smtp.163.com", port=994)
    smtp.login(sender, sender_auth)
    smtp.sendmail(sender, recipient, message.as_string())
    smtp.close()
    

def format_messages_to_html(data):
    # print(data)
    A = data.iat[0, 1]
    B = data.iat[1, 1]
    C = data.iat[2, 1]
    D = data.iat[3, 1]
    A = [f'<a href="https://finance.sina.com.cn/futures/quotes/{i}.shtml"> {i} | </a>' for i in A]
    As=''
    for i in A:
        As += i
    B = [f'<a href="https://finance.sina.com.cn/futures/quotes/{i}.shtml"> {i} | </a>' for i in B]
    Bs=''
    for i in B:
        Bs += i
    C = [f'<a href="https://finance.sina.com.cn/futures/quotes/{i}.shtml"> {i} | </a>' for i in C]
    Cs=''
    for i in C:
        Cs += i
    D = [f'<a href="https://finance.sina.com.cn/futures/quotes/{i}.shtml"> {i} | </a>' for i in D]
    Ds=''
    for i in D:
        Ds += i
    if As == '':
        As = '无'
    if Bs == '':
        Bs = '无'
    if Cs == '':
        Cs = '无'
    if Ds == '':
        Ds = '无'
    messages = """\
    
        <html>
        <head>
        <style>
        table, th, td {{
        border:1px solid black;
        }}
        </head>
        </style>
        <body>
            <table style="width:100%">
            <tr>
                <th>评估指标</th>
                <th>评估结果</th>
            </tr>
            <tr>
                <td>正基差</td>
                <td>{As}</td>
            </tr>
            <tr>
                <td>价格高于20日均线，且日KDJ交金叉</td>
                <td>{Bs}</td>
            </tr>
            <tr>
                <td>价格高于60分钟-20均线，且60分钟-KDJ交金叉</td>
                <td>{Cs}</td>
            </tr>
            <tr>
                <td><b><p style="color:red;">综合结果</p></b></td>
                <td>{Ds}</td>
            </tr>
            </table>
        </body>
        </html>
    """
    messages = messages.format(As=As,Bs=Bs,Cs=Cs,Ds=Ds)
    return messages




if __name__ == '__main__':
    # email = '13051994355@163.com'
    data = main()
    sender = "13091171683@163.com"
    sender_auth = "OUBDVKZHNFSBZUXY"
    recipient = "lei.lei.fan.meng@gmail.com"
    messages = format_messages_to_html(data)
    # print(messages)
    send_email(messages, sender, sender_auth, recipient)



