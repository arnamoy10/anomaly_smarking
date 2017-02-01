import sys
    
#print "Importing libraries"
import os
import json
import numpy as np
from pandas import bdate_range, DataFrame, date_range, to_datetime, Series

import requests
from datetime import datetime, timedelta
from dateutil import relativedelta


#STAGE 1: Perform some preprocessing before running main()

import smarking_globals

#We do one garage at a time so, just one iteration is fine
#TODO: take out the loop and fix indentation

#change the authentication token accordingly
if ('Bearer' not in os.environ):
    print ("Please set the Bearer environment variable")
    sys.exit(0)
smarking_globals.bearer = "Bearer "+ str(os.environ['Bearer'])
smarking_globals.headers = {"Authorization":smarking_globals.bearer}

def get_json_info(url):
    #get the response using the url
    response = requests.get(url,headers=smarking_globals.headers)
    content = response.content

    #see if content was received.  If nothing  received, exit
    if (content == ""):
        print ("No content received")
        sys.exit(0)

    #we have collected all the data
    #each datapoint is for an hour in a given day
    try:
        garage_info = json.loads(content)
    except ValueError:
        print ("No JSON Object received for occupancy, please try again.")
        sys.exit(0)
    return garage_info


def get_occupancy_data():    
    #get the duration for the supplied date range so that we can create the URL

    delta= smarking_globals.end_date - smarking_globals.start_date

    duration_hours = 0
    if delta.days == 0:
        duration_hours = 24
    else:
        duration_hours = (abs(delta.days)+1) * 24
    
    url="https://my.smarking.net/api/ds/v3/garages/"+str(smarking_globals.garage_id)\
    +"/past/occupancy/from/"+smarking_globals.start_date_supplied+"T00:00:00/"\
    +str(duration_hours)+"/1h?gb=User+Type"

    garage_info = get_json_info(url)
    
    if 'value' not in garage_info:
        print ("No valid information received")
        sys.exit(0)
    
    #parse the JSON-formatted line
    for item in garage_info["value"]:
        #check if value contains anything
 
        group = str(item.get("group"))
        
        smarking_globals.occupancies_all_groups.append(item.get("value"))
        smarking_globals.names_all_groups.append(group)
    
def get_duration_data():
    
    url = "https://my.smarking.net/api/ds/v3/garages/"+str(smarking_globals.garage_id)\
    +"/past/duration/between/"+smarking_globals.start_date_supplied+"T00:00:00/"\
    +str((to_datetime(smarking_globals.end_date_supplied)+timedelta(1)).date())\
    +"T00:00:00?bucketNumber=25&bucketInSeconds=600&gb=User+Type"

    #get the response using the url
    garage_info = get_json_info(url)
    
    #parse the JSON-formatted line
    for item in garage_info["value"]:   
        smarking_globals.durations_all_groups.append(item.get("value"))
    
    #set_garage_info_duration(con_dur, tran_dur)    
    
def get_holidays():
    years_t=[]
    start_year = smarking_globals.start_date.year
    end_year = smarking_globals.end_date.year
            
    if (start_year == end_year):
        years_t.append(start_year)
    else:
        years_t.append(start_year)
        years_t.append(end_year)
    for year_t in years_t:
        #filename
        f_name = "holidays"+str(year_t)
        if(os.path.isfile(f_name) != True):
            print ("%s file not present, needed for analysis"%f_name)
            sys.exit(0)
        with open(f_name) as f:
            hdays= f.readlines()
        for mmm in hdays:
            dt = datetime.strptime(mmm.rstrip('\n'), smarking_globals.date_format)
            smarking_globals.holidays.append(str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)) 

def get_months():
    delta= relativedelta.relativedelta(smarking_globals.start_date, smarking_globals.end_date)
    months = delta.months
    years = delta.years

    smarking_globals.total_months = abs(years)*12+abs(months)