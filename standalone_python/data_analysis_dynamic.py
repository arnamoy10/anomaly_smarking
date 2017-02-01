import sys

if(len(sys.argv) < 4):
    print ("Usage: python data_analysis.py <garage_id> <start_date> <end_date>")
    print ("start and end date should be in YYYY-mm-dd format")
    sys.exit(0)

    
#print "Importing libraries"
import os
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

#STAGE 1: Perform some preprocessing before running main()

# Get the garage_name, start and end dates
smarking_globals.garage_id = sys.argv[1]
smarking_globals.start_date_supplied = sys.argv[2]
smarking_globals.end_date_supplied = sys.argv[3]


#We do one garage at a time so, just one iteration is fine
#TODO: take out the loop and fix indentation

#change the authentication token accordingly
if ('Bearer' not in os.environ):
    print ("Please set the Bearer environment variable")
    sys.exit(0)

def main():            
    print ("  Processing Garage", smarking_globals.garage_id) 
    
    try:
        smarking_globals.start_date = datetime.strptime(smarking_globals.start_date_supplied, smarking_globals.date_format)
        smarking_globals.end_date = datetime.strptime(smarking_globals.end_date_supplied, smarking_globals.date_format)
    except ValueError:
        print ("Incorrect date format, should be YYYY-mm-dd")
        sys.exit(0)

    #check if the from_date is < to_date
    if (smarking_globals.start_date > smarking_globals.end_date):
        print ("From_date can't be after to_date")
        sys.exit(0)
        
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
        print ('\033[92m'+"Hurray, no anomalies"+'\033[0m')
    else:
        print ('\033[91m'+"Time to fix the follwing anomalies"+'\033[0m')
        for item in smarking_globals.anomalies_for_google_docs:
            print (item[0],item[1]) 

if __name__ == "__main__":   
    main()