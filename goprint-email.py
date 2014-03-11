#!/usr/bin/python

## use yum to install python setup tools
## then, as root, type easy_install pip
## (also as root) pip install psycopg2
import psycopg2
import sys
import os

# imports for actually sending the email
import smtplib
from email.mime.text import MIMEText

# import to check date
from datetime import date, timedelta

# All the database information goes here
DATABASE = '127.0.0.1'
DB_NAME = 'goprint'
USER = 'goprint_email'
PASSWORD = os.environ['GOPRINTPASS']

# List of lists of users that were contacted
# each follows the form ([Current Balance, Account ID, First Name, Last Name])
USERS_CONTACTED = []

# Setting up variable in order to determine previous
ONE_DAY = timedelta(days=1)


# Contacts the goprint database and grabs all the needed information
def main():
   txtfile = 0
   try:
      txtfile = open("lastran.txt", "r")
   except IOError:
      LAST_CONTACTED = str(date.today() - ONE_DAY)
   if txtfile != 0:
      LAST_CONTACTED = str(txtfile.readline())
      print "Last contacted on " + LAST_CONTACTED


   # Define our connection string
   conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % (DATABASE, DB_NAME, USER, PASSWORD)

   # print the connection string we will use to connect
   print "Connecting to database\n   ->%s" % (conn_string)

   # get a connection, if a connect cannot be made an exception will be raised here
   try:
      conn = psycopg2.connect(conn_string)
   except Exception, e:
      print(e)

   # conn.cursor will return a cursor object, you can use this cursor to perform queries
   cursor = conn.cursor()
   print "Connected!\n"

   # rows is a list of lists containing all of the account information needed
   rows = retrieve_rows_from_table(cursor, True)
   if LAST_CONTACTED != str(date.today() - ONE_DAY) and LAST_CONTACTED != str(date.today()):
      print "Not up to date"
      rows.extend(retrieve_rows_from_table(cursor, False, LAST_CONTACTED))
   rows = sorted(rows, key=lambda row: row[0])

   parse_through_current_table(rows)

   if len(USERS_CONTACTED) != 0:
      send_report(USERS_CONTACTED)

   txtfile = open("lastran.txt", "w")
   txtfile.seek(0)
   txtfile.write(str(date.today()))
   txtfile.truncate()
   txtfile.close()


# Runs a postgreSQL query to grab all the needed information for the rest of the program
def retrieve_rows_from_table(cursor, today, LAST_CONTACTED = "NA"):
   if today:
      cursor.execute('''
         SELECT acct.AccountKey, acct.AccountID, cl.ClassKey, purse.PurseKey,
         u.LastName, u.FirstName,
         lgr.Trandate, lgr.Debit,
         coalesce(ab.Balance,0) + coalesce(ab.PurgedBalance,0) +
         coalesce((select sum(Credit-Debit)
         FROM tranledger tl
         WHERE tl.AccountKey = acct.AccountKey AND tl.PurseKey = purse.PurseKey AND tl.AccountType = 'B'),0) AS Balance
         FROM account acct
         INNER JOIN goprintuser u ON acct.AccountKey = u.AccountKey
         INNER JOIN classmember cm ON acct.AccountKey = cm.AccountKey
         INNER JOIN purse purse ON cm.ClassKey = purse.ClassKey AND purse.Active = 'Y'
         INNER JOIN class cl ON cm.ClassKey = cl.ClassKey AND cl.ClassType = 'U'
         INNER JOIN tranledger lgr ON acct.AccountKey = lgr.AccountKey AND purse.PurseKey = lgr.PurseKey
         LEFT JOIN accountbalance ab ON acct.AccountKey = ab.AccountKey AND purse.PurseKey = ab.PurseKey AND ab.AccountType = 'B'
         WHERE u.Active = 'Y' AND lgr.Debit > 0.000 AND cl.ClassKey = 20 AND purse.PurseKey = 45
         '''
         )
   else:
      cursor.execute('''
         SELECT acct.AccountKey, acct.AccountID, cl.ClassKey, purse.PurseKey,
         u.LastName, u.FirstName,
         hist.Trandate, hist.Debit,
         coalesce(ab.Balance,0) + coalesce(ab.PurgedBalance,0) +
         coalesce((select sum(Credit-Debit)
         FROM tranledger tl
         WHERE tl.AccountKey = acct.AccountKey AND tl.PurseKey = purse.PurseKey AND tl.AccountType = 'B'),0) AS Balance
         FROM account acct
         INNER JOIN goprintuser u ON acct.AccountKey = u.AccountKey
         INNER JOIN classmember cm ON acct.AccountKey = cm.AccountKey
         INNER JOIN purse purse ON cm.ClassKey = purse.ClassKey AND purse.Active = 'Y'
         INNER JOIN class cl ON cm.ClassKey = cl.ClassKey AND cl.ClassType = 'U'
         INNER JOIN tranhistory hist ON acct.AccountKey = hist.AccountKey AND purse.PurseKey = hist.PurseKey
         LEFT JOIN accountbalance ab ON acct.AccountKey = ab.AccountKey AND purse.PurseKey = ab.PurseKey AND ab.AccountType = 'B'
         WHERE u.Active = 'Y' AND hist.Debit > 0.000 AND cl.ClassKey = 20 AND purse.PurseKey = 45 AND hist.updatebyuserid != 'EXTERNAL AUTHENTICATION' AND hist.TranDate > ''' + "'" + LAST_CONTACTED + " 00:00:00.0'"
         )

   return cursor.fetchall()


# Runs through the table grabbing out all the rows and adding all identical users' debits together
def parse_through_current_table(rows):
   account_key = -1
   account_id = ''
   class_key = 0
   purse_key = 0
   last_name = ''
   first_name = ''
   debit = 0
   current_balance = -1

   txtfile = open("useraccounts.txt", "w")
   txtfile.seek(0)

   for row in rows:
      if account_key == -1:
         account_key = row[0]
         account_id = row[1]
         class_key = row[2]
         purse_key = row[3]
         last_name = row[4]
         # because darn those people who don't have first names for some reason
         try:
            first_name = row[5].split()[0]
         except AttributeError:
            first_name = last_name
         debit = float(row[7])
         current_balance = float(row[8])

      elif account_key == row[0] and purse_key == row[3]:
         debit = debit + float(row[7])

      else:
         if current_balance < 0.05 and (current_balance + debit) >= 0.05:
            send_email(account_id, last_name, first_name, current_balance, 0)
            current_balance = str(round(current_balance,2))
            if len(current_balance.split(".")[1]) == 1:
               current_balance = current_balance + "0" 
            USERS_CONTACTED.append([current_balance, account_id, first_name, last_name])

         elif current_balance < 1.0 and (current_balance + debit) >= 1.0:
            send_email(account_id, last_name, first_name, current_balance, 1)
            current_balance = str(round(current_balance,2))
            if len(current_balance.split(".")[1]) == 1:
               current_balance = current_balance + "0" 
            USERS_CONTACTED.append([current_balance, account_id, first_name, last_name])

         elif current_balance < 2.0 and (current_balance + debit) >= 2.0:
            send_email(account_id, last_name, first_name, current_balance, 2)
            current_balance = str(round(current_balance,2))
            if len(current_balance.split(".")[1]) == 1:
               current_balance = current_balance + "0" 
            USERS_CONTACTED.append([current_balance, account_id, first_name, last_name])

         txtfile.write(str(account_key) + " " + account_id + " " + first_name + " " + last_name + " " + str(purse_key) + " " + str(debit) + " " + str(current_balance) + "\n")
         account_key = row[0]
         account_id = row[1]
         class_key = row[2]
         purse_key = row[3]
         last_name = row[4]
         # See line 141
         try:
            first_name = row[5].split()[0]
         except AttributeError:
            first_name = last_name
         debit = float(row[7])
         current_balance = float(row[8])

   txtfile.write(str(account_key) + " " + account_id + " " + first_name + " " + last_name + " " + str(purse_key) + " " + str(debit) + " " + str(current_balance) + "\n")
   if current_balance < 0.05 and (current_balance + debit) >= 0.05:
      send_email(account_id, last_name, first_name, current_balance, 0)
      current_balance = str(round(current_balance,2))
      if len(current_balance.split(".")[1]) == 1:
         current_balance = current_balance + "0" 
      USERS_CONTACTED.append([current_balance, account_id, first_name, last_name])

   elif current_balance < 1.0 and (current_balance + debit) >= 1.0:
      send_email(account_id, last_name, first_name, current_balance, 1)
      current_balance = str(round(current_balance,2))
      if len(current_balance.split(".")[1]) == 1:
         current_balance = current_balance + "0" 
      USERS_CONTACTED.append([current_balance, account_id, first_name, last_name])

   elif current_balance < 2.0 and (current_balance + debit) >= 2.0:
      send_email(account_id, last_name, first_name, current_balance, 2)
      current_balance = str(round(current_balance,2))
      if len(current_balance.split(".")[1]) == 1:
         current_balance = current_balance + "0" 
      USERS_CONTACTED.append([current_balance, account_id, first_name, last_name])

   txtfile.truncate()
   txtfile.close()


# Default message based on money left in account
def email_msg(first_name, current_balance):
   if len(current_balance.split(".")[1]) == 1:
      current_balance = current_balance + "0" 
   msg = '''Hello %s,\n
Your GoPrint balance is currently $%s. Please visit the Office for Financial Services during business hours to add money to your NordiCash account. See http://www.luther.edu/lis/goprint for information on after hours printing.\n
If you have any questions, please contact the LIS Technology Help Desk at 563-387-1000, helpdesk@luther.edu, or enter you request online at https://help.luther.edu''' % (first_name, current_balance)
   return msg

# Python alternative to a switch statement
#AMOUNT_LEVEL = {0 : zero_dollar_msg , 1 : one_dollar_msg , 2 : two_dollar_msg}


# As definition implies, this sends an email to the user
def send_email(email, last_name, first_name, current_balance, money_amount):
   sender = "helpdesk@luther.edu"
   receiver = email + "@luther.edu"

   # Modifying current balance so it's not just crazy broken
   current_balance = str(round(current_balance,2))

   # Create a string to use as the message body.
   #message = AMOUNT_LEVEL[money_amount](first_name, current_balance)
   message = email_msg(first_name, current_balance)


   # Create a text/plain message.
   msg = MIMEText(message)

   if money_amount != 0:
      msg['Subject'] = "GoPrint Balance Alert: Under $%s remaining" % (money_amount)
   else:
      msg['Subject'] = "GoPrint Balance Alert: Balance at $%s" % (money_amount)
   msg['From'] = sender
   msg['To'] = "markga01@luther.edu, carsten@luther.edu, gossmand@luther.edu" #receiver

   # Setting up smtp with mail server. When left blank it defaults to localhost.
   s = smtplib.SMTP()
   s.connect()
   s.sendmail(sender, ["markga01@luther.edu","carsten@luther.edu", "gossmand@luther.edu"], msg.as_string())
   s.quit()


# As definition implies, this sends a report to the HD Managers at the end of each day
def send_report(users):
   if len(users) == 1:
      print "Sending report of one user contacted today"
   else:
      print "Sending report of " + str(len(users)) + " users contacted today"
   sender = "goprint@luther.edu"
   receiver = "markga01@luther.edu" #"hdmanagers@luther.edu"

   users = sorted(users, key=lambda user: user[1])

   # Create the string for the report email body.
   message = "These are all the students who were contacted and their current balances.\n"
   for user in users:
      message = message + "\n" + "$" + user[0] + (" " * (10 - len(user[0]))) + user[1] + (" " * (15 - len(user[1]))) + user[2] + " " + user[3]

   # Create a text/plain message.
   msg = MIMEText(message)

   msg['Subject'] = "GoPrint Student Balance Report"
   msg['From'] = sender
   msg['To'] = receiver + ", carsten@luther.edu, gossmand@luther.edu"

   # Setting up smtp with mail server. When left blank it defaults to localhost.
   s = smtplib.SMTP()
   s.connect()
   s.sendmail(sender, [receiver, "carsten@luther.edu", "gossmand@luther.edu"], msg.as_string())
   s.quit()


if __name__ == "__main__":
   main()
