#!/usr/local/bin/python3

import sys

print ("""Content-Type: text/html\n
<html>

<head><meta charset="UTF-8"><title>Smarking checking</title> <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/meyer-reset/2.0/reset.min.css"><link rel='stylesheet prefetch' href='http://fonts.googleapis.com/css?family=Roboto:400,100,300,500,700,900|RobotoDraft:400,100,300,500,700,900'><link rel='stylesheet prefetch' href='http://maxcdn.bootstrapcdn.com/font-awesome/4.3.0/css/font-awesome.min.css'><link rel="stylesheet" href="/css/style.css">
</head>

""")

print ("""
<body>
<div class="pen-title">
    <p><img src="/logo-s.png"><h2 style="color:darkcyan;font-size: 20px;">Please hang on while we run our analysis. </h2></p></div>
""")
sys.stdout.flush()

    
#print "Importing libraries"
import cgi, os
import cgitb; cgitb.enable()
import json
import numpy as np
from pandas import bdate_range, DataFrame, date_range, to_datetime, Series

import requests
from datetime import datetime, timedelta
from dateutil import relativedelta


#twitter analysis for timeseries anomaly detection
from pyculiarity import detect_vec

import smarking_globals
import smarking_get_data
import smarking_anomaly_detection
import smarking_google_stuff
import smarking_checks


#STAGE 1: Perform some preprocessing before running main()

form = cgi.FieldStorage()

#STAGE 1: Perform some preprocessing before running main()

# Get the garage_name, start and end dates
smarking_globals.garage_id = str(form.getfirst('garage_id'))
smarking_globals.start_date_supplied = str(form.getfirst('start_date'))
smarking_globals.end_date_supplied = str(form.getfirst('end_date'))



def main():  
    print ("<p style='color:darkcyan;font-size: 22px;'>    Processing Garage", smarking_globals.garage_id,"</p>")
    sys.stdout.flush() 
    
    #perform various checks
    smarking_checks.check()
        
    #get the holidays
    smarking_get_data.get_holidays() 

    #STAGE 2: Getting the data
    smarking_get_data.get_occupancy_data()
    smarking_get_data.get_duration_data()
    
    smarking_get_data.get_months()

    delta= smarking_globals.end_date - smarking_globals.start_date
    ndays = abs(delta.days)+1
        
    #anomaly detection
    for ii in np.arange(0, len(smarking_globals.occupancies_all_groups)):
        if (smarking_globals.total_months > 6):
            smarking_anomaly_detection.calculate_monthly_peak_anomaly\
            (smarking_globals.occupancies_all_groups[ii], smarking_globals.names_all_groups[ii])
        if (ndays > 20):
            smarking_anomaly_detection.calculate_dp_anomaly\
            (ndays, smarking_globals.occupancies_all_groups[ii], smarking_globals.names_all_groups[ii]) 
        #we choose 18 because we need at least 
        #some data points to get signal properties
        if (ndays > 18):
            smarking_anomaly_detection.calculate_daily_indiv\
            (ndays, smarking_globals.occupancies_all_groups[ii], smarking_globals.names_all_groups[ii])
        if (ndays > 18):
            smarking_anomaly_detection.calculate_overnight_anomaly\
            (ndays, smarking_globals.occupancies_all_groups[ii], smarking_globals.names_all_groups[ii])
            
        smarking_anomaly_detection.calculate_duration_anomalies\
        (smarking_globals.occupancies_all_groups[ii], smarking_globals.names_all_groups[ii])
        
    #print detected anomalies
    if (len(smarking_globals.anomalies_for_google_docs) == 0):
        print ("<p style='color:darkcyan;font-size: 22px;'>Hurray, no anomalies</p>")
        sys.stdout.flush()
    else:
        print ("<p style='color:darkcyan;font-size: 22px;'>We found some anomalies</p>")
        #for item in smarking_globals.anomalies_for_google_docs:
        #    print ("<p>",item,"</p>") 
        argument=str(smarking_globals.garage_name.rstrip("\n"))+"_"+str(smarking_globals.garage_id.rstrip("\n"))
        smarking_google_stuff.write_to_google_doc(argument)
        print ("""
<br><br><h2 style="color:darkcyan;font-size: 20px;">The anomalies can be found <a href="https://docs.google.com/spreadsheets/d/1zZ0XS0yDKLK9YkWeimEgv41u-7EP6_YzsyHPp_o-MBs/edit?usp=sharing" target = _blank>here</a></h2><br><br>
<p><a href = "../index.html">Go back to home page</a></p>
</body>
</html>""")

if __name__ == "__main__":   
    main()