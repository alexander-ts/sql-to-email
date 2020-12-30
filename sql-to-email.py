import json 
import pyodbc 
import csv 
from smtplib import SMTP_SSL
from email.message import EmailMessage
from datetime import datetime
from datetime import date
import os
import sys
import logging

class SQLManager:
    def __init__(self, dsn, username, password):
        connection = pyodbc.connect(f'DSN={dsn};UID={username};PWD={password}')
        self._cursor = connection.cursor()

    def getDataHeaders(self):
        return self._columns
    
    def getDataRows(self):
        return self._rows

    def executeReport(self, path):
        try:
            with open(path + "sql.sql", 'r') as sql:
                self._cursor.execute(sql.read())
                self._columns = [column[0] for column in self._cursor.description]
                self._rows = self._cursor.fetchall()
                print(self._rows)
        except FileNotFoundError:
            print(f'SQL-file not found! Path: {path}.')

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

class FileManager():
    #Not used.
    def createDirectoryIfNotExists(self, path):
        if not (os.path.isdir(path)):
            os.mkdir(path)

    def saveToCSV(self, filename, columnNames, rows, isHeadersIncluded):
        with open(filename, 'w+', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            if (isHeadersIncluded):
                writer.writerow(columnNames)
            writer.writerows(rows)

class ConfigurationManager():
    def __init__(self, filename):
        with open(filename, 'r', encoding='windows-1251') as f:
            self.configuration = json.loads(''.join(f.readlines()))

    def printReports(self):
        columnNames = ['DSN', 'Report']
        columnWidth = 30
        header = ''
        for column in columnNames:
            header += column + (columnWidth - len(column)) * ' '
        print(header)
        print('-' * len(header))

        for cnxn in self.configuration['dbConnections']:
            for report in cnxn['reports']:
                row = f'{cnxn["dsn"] + " " * (columnWidth - len(cnxn["dsn"]))}' + report["name"]
                print(row)

    def getReport(self, dsn, reportName):
        for cnxn in self.configuration['dbConnections']:
            if cnxn['dsn'] == dsn:
                for report in cnxn['reports']:    
                    if report['name'] == reportName:
                        return report

    def getConnection(self, dsn):
        for cnxn in self.configuration['dbConnections']:
            if cnxn['dsn'] == dsn:
                return cnxn

if __name__ == "__main__":
    logging.basicConfig(filename=f'logs/{date.today().strftime("%Y%m%d")}.log', level="INFO")
    try:
        args = sys.argv[1:]
        configurationManager = ConfigurationManager('configuration.json')

        logging.info(f'New call: {sys.argv}')

        if (len(args) == 2):
            cnxn = configurationManager.getConnection(args[0])
            report = configurationManager.getReport(args[0], args[1])
            
            smtpSettings = configurationManager.configuration['smtp']
            emailManager = EmailManager(smtpSettings['smtpHost'], smtpSettings['smtpPort'], smtpSettings['email'], smtpSettings['password'])
            csvManager = FileManager()
            sqlManager = SQLManager(cnxn['dsn'], cnxn['login'], cnxn['password'])
            path = report['path'] + f"csv/[{date.today().strftime('%d.%m.%Y %h:%mm')}] {report['name']}"

            sqlManager.executeReport(report['path'])
            csvManager.saveToCSV(path, columnNames=sqlManager.getDataHeaders(), rows=sqlManager.getDataRows(), isHeadersIncluded=report['isHeadersIncluded'])
            emailManager.send(subject=report['name'], fromEmail=smtpSettings['email'], toEmail=report['recipent'], filename=path)
        else:
            print('You need to specify DSN name and report to execute! \n')
            print('Example: \npython sql-to-email.py DSN_NAME example-report-name\n')
            print('List of available reports:\n')
            configurationManager.printReports()
    except Exception as e:
        print('Something went wrong. See logs for details.')
        logging.error(e)