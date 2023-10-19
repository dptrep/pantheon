'''
Created on Apr 27, 2019

@author: dan

based on: https://realpython.com/python-send-email/
'''

import email.message
import smtplib
import ssl
from src import settings 


def send(subject, message):
    # Create a secure SSL context
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", settings.MAIL['port'], context=context) as server:
        server.login(settings.MAIL['user'], settings.MAIL['password'])
        server.sendmail(settings.MAIL['user'], 'dan@trepcapital.com', 'Subject: %s\n\n%s' % (subject , message))
    # TODO: Send email here
    
def send_html(subject, content):
    msg = email.message.Message()
    msg['Subject'] = subject
    msg['From'] = 'trepcapital@gmail.com'
    msg['To'] = 'dan@trepcapital.com'
    msg.add_header('Content-Type','text/html')
    msg.set_payload(content)
    
    # Send the message via local SMTP server.
    #context = ssl.create_default_context()
    s = smtplib.SMTP('smtp.gmail.com', settings.MAIL['port'])
    s.ehlo()
    s.starttls()
    s.login(settings.MAIL['user'],
            settings.MAIL['password'])
    s.sendmail(msg['From'], [msg['To']], msg.as_string())
    s.quit()
    
    
if __name__ == "__main__":
    send('Hello World','Hi Dan,\n\nThis e mail is sent from the src.core.mail application')
    
    