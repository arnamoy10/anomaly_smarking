#!/usr/bin/python

import sys

print """Content-Type: text/html\n
<html>

<head><meta charset="UTF-8"><title>Smarking checking</title> <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/meyer-reset/2.0/reset.min.css"><link rel='stylesheet prefetch' href='http://fonts.googleapis.com/css?family=Roboto:400,100,300,500,700,900|RobotoDraft:400,100,300,500,700,900'><link rel='stylesheet prefetch' href='http://maxcdn.bootstrapcdn.com/font-awesome/4.3.0/css/font-awesome.min.css'><link rel="stylesheet" href="/css/style.css">
</head>

"""

print """
<body>
<div class="pen-title">
    <p><img src="/logo-s.png"><h2 style="color:darkcyan;font-size: 20px;">Adding the Garage. </h2></p></div>
"""
sys.stdout.flush()

import cgi, os
import cgitb; cgitb.enable()

    
#print "Importing libraries"
import os
import requests


#Google doc stuff
import httplib2

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart' #you can modify it in the 
#API Url

form = cgi.FieldStorage()

# Get filename here.
garage_id = form.getfirst('garage_id')
garage_url = form.getfirst('garage_url')
garage_name = form.getfirst('garage_name')


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    credential_dir = os.getcwd()
    credential_path = os.path.join(credential_dir,
                                   'smarking_error_check.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def write_to_google_doc(garage_id, garage_name, garage_url):
    """writes to the spreadsheet
    """
    flag = 0
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)
    
    spreadsheet_historical = '1zZ0XS0yDKLK9YkWeimEgv41u-7EP6_YzsyHPp_o-MBs'
    spreadsheet_realtime = '1mVQnF2Gic967faCyPR8vBoLoLVkB6rs-Cv0S5amy8mI'
    
    #create the worksheet name
    sheet_title=str(garage_name)+"_"+str(garage_id)
    
    #print rangeName
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_historical).execute()
    sheets = metadata.get('sheets', '')
    for sheet in sheets:
        title = sheet.get("properties", {}).get("title", "Sheet1")
        if(str(sheet_title) == str(title)):
            print "<p>The garage is already present in historical spreadsheet"
            sys.stdout.flush()
            flag = 1
            
    if (flag == 0):
        #sheet not present
        requests = []
        # Change the spreadsheet's title
        
        requests.append({
                "addSheet": {
                    "properties": {
                        "title": sheet_title,
                        "sheetType": "GRID",
                        "gridProperties": {
                            "rowCount": 10000,
                            "columnCount": 10
                        }
                    }
                }
            })
        body = {
            'requests': requests
        }
        response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_historical,
                                               body=body).execute()
        #add the header row
    
    
        values=[]
        value=[]
        value.append("Date")
        value.append("Type")
        value.append("url")
        value.append("Checked")
        value.append("FP?")
        value.append("Checked by")
        value.append("Comments")
        values.append(value)
        body = {
            'values': values
        }
        rangeName = sheet_title +'!A1:G'
        result = service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_historical, range=rangeName,
                valueInputOption='USER_ENTERED', body=body).execute()
    
        print "<p>Added in Historical Spreadsheet"
        sys.stdout.flush()
    
    flag = 0
       
    
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_realtime).execute()
    sheets = metadata.get('sheets', '')
    for sheet in sheets:
        title = sheet.get("properties", {}).get("title", "Sheet1")
        if(str(sheet_title) == str(title)):
            print "<p>The garage is already present in Realtime Spreadsheet"
            sys.stdout.flush()
            flag = 1
            
    if (flag == 0):
        #sheet not present
        requests = []
        # Change the spreadsheet's title
        
        requests.append({
                "addSheet": {
                    "properties": {
                        "title": sheet_title,
                        "sheetType": "GRID",
                        "gridProperties": {
                            "rowCount": 10000,
                            "columnCount": 10
                        }
                    }
                }
            })
        body = {
            'requests': requests
        }
        response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_realtime,
                                               body=body).execute()
        #add the header row
    
    
        values=[]
        value=[]
        value.append("Date")
        value.append("Type")
        value.append("url")
        value.append("Checked")
        value.append("FP?")
        value.append("Checked by")
        value.append("Comments")
        values.append(value)
        body = {
            'values': values
        }
        rangeName = sheet_title +'!A1:G'
        result = service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_realtime, range=rangeName,
                valueInputOption='USER_ENTERED', body=body).execute()
        
        print "<p>Added in Realtime Spreadsheet"
        sys.stdout.flush()
    
def write_to_garage_info_file(garage_id, garage_name, garage_url):
    #get the garage names
    garage_dict={}
    with open("garage_names") as f:
        for line in f:
            (key, x, val) = line.split(",")
            garage_dict[int(key)] = val
    if int(garage_id) in garage_dict:
        print "<p> Garage already present in garage info file</p>"
        return
    string = str(garage_id)+","+str(garage_url)+","+str(garage_name)+"\n"
    with open("garage_names", "a") as myfile:
        myfile.write(string)
    
write_to_google_doc(garage_id, garage_name, garage_url)
write_to_garage_info_file(garage_id, garage_name, garage_url)

print """
<h2 style="color:darkcyan;font-size: 25px;">Successfully added Garage %s. </h2></p></div>
<p><a href = "../index.html">Go back to home page</a></p>
</body>
</html>""" % garage_name
        
    


