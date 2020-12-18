import json 
import pyodbc 
import csv 
from smtplib import SMTP_SSL
from email.message import EmailMessage

class SQLManager:
    def __init__(self, dsn, username, password):
        connection = pyodbc.connect(f'DSN={dsn};UID={username};PWD={password}')
        self._cursor = connection.cursor()

    def getDataHeaders(self):
        return self._columns
    
    def getDataRows(self):
        return self._rows

    def executeSQL(self, sql):
        self._cursor.execute(sql)
        self._columns = [column[0] for column in self._cursor.description]
        self._rows = self._cursor.fetchall()

class EmailManager:
    def __init__(self, host, port, login, password):
        self._smtp = SMTP_SSL(host=host, port=port)
        self._smtp.login(login, password)

    def send(self, subject, fromEmail, toEmail, filename):
        message = EmailMessage()
        message['Subject'] = subject
        message['From'] = fromEmail
        message['To'] = toEmail

        with open(filename, 'rb') as attachment:
            csv_data = attachment.read()

        message.add_attachment(csv_data, maintype="text", subtype="csv")
        self._smtp.sendmail(from_addr=fromEmail, to_addrs=toEmail, msg=message.as_bytes())

class CSVManager():
    def saveToCSV(self, filename, columnNames, rows):
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columnNames)
            writer.writerows(rows)

def getConfiguration():
    with open('configuration.json', 'r', encoding='utf-8') as f:
        return ''.join(f.readlines())

configuration = json.loads(getConfiguration())
smtpSettings = configuration['smtp']
emailManager = EmailManager(smtpSettings['smtpHost'], smtpSettings['smtpPort'], smtpSettings['email'], smtpSettings['password'])
csvManager = CSVManager()

for cnxn in configuration['dbConnections']:
    sqlManager = SQLManager(cnxn['dsn'], cnxn['login'], cnxn['password'])
    for report in cnxn['reports']:
        sqlManager.executeSQL(report['sql'])
        csvManager.saveToCSV(report['path'] + report['filename'], columnNames=sqlManager.getDataHeaders(), rows=sqlManager.getDataRows())
        emailManager.send(subject=report['name'], fromEmail=smtpSettings['email'], toEmail=report['recipient'], filename=report["path"] + report["filename"])