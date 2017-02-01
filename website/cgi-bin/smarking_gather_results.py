import sys

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


def gather_anomalies_monthly_peak(dates,indices, group_currently_processing,zero_iqr):
        
    anomaly_dates = dates[indices]
    for anom_date in anomaly_dates:
        #create the anomaly and add to master list
        temp_anomaly=[]
        mon = str(anom_date.year)+"-"+str(anom_date.month)
        if(zero_iqr == 0):
            anom_type = group_currently_processing \
                        + " zero monthly peak"
            #also store in monthly_peak_zero for daily_peak_zero analysis
            smarking_globals.monthly_peak_zero.append(mon)
        elif(zero_iqr == 1):
            anom_type = group_currently_processing \
                        + " unusual monthly peak"
        else:
            anom_type = group_currently_processing \
                        + " trend change in monthly peak"
                
        url = "https://my.smarking.net/rt/"+smarking_globals.garage_url.rstrip('\n')\
        +"/occupancy?granularity=Monthly&fromDateStr="+str(smarking_globals.start_date)\
        +"&toDateStr="+str(smarking_globals.end_date)
                    
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)
        temp_anomaly.append(url)
        temp_anomaly.append("No")
        temp_anomaly.append("No")

        smarking_globals.anomalies_for_google_docs.append(temp_anomaly)
            
def gather_anomalies_daily_peak(dates,indices, group_currently_processing,zero_iqr):
    
    anomaly_dates = dates[indices]

    for anom_date in anomaly_dates:
        temp_anomaly =[]
        if ((str(anom_date.year)+"-"+str(anom_date.month)+"-"+str(anom_date.day)) not in smarking_globals.holidays):
            mon = str(anom_date.year)+"-"+str(anom_date.month)+"-"+str(anom_date.day)
            #print mon
            found = 0
            if(zero_iqr == 0):
                anom_type = group_currently_processing+" zero daily-peak"
                if (str(anom_date.year)+"-"+str(anom_date.month)) in smarking_globals.monthly_peak_zero:
                    #print "found in"
                    found = 1
            elif(zero_iqr == 1):
                anom_type = group_currently_processing+" unusual daily-peak"
            else:
                anom_type = group_currently_processing+" trend change in daily-peak"
            
            if (found == 0):
                url = "https://my.smarking.net/rt/"+smarking_globals.garage_url.rstrip('\n')\
                        +"/occupancy?granularity=Monthly&fromDateStr="\
                        +str(smarking_globals.start_date) +"&toDateStr="+\
                        str(smarking_globals.end_date)
                    
                temp_anomaly.append(mon)
                temp_anomaly.append(anom_type)
                temp_anomaly.append(url)
                temp_anomaly.append("No")
                temp_anomaly.append("No")

                smarking_globals.anomalies_for_google_docs.append(temp_anomaly)
                #also add to the daily peak anomalies data struct
                smarking_globals.daily_peak_anomalies.append(mon+group_currently_processing)
                                    
def gather_daily_anomaly(result_dates, group_currently_processing):
    df = DataFrame({'date': result_dates})
    df1=df.drop_duplicates('date')
                
    for row in df1.iterrows():
        if ((str(row[1].date.year)+"-"+str(row[1].date.month)+"-"+str(row[1].date.day)) not in smarking_globals.holidays):
            temp_anomaly=[]
            mon = str(row[1].date.year)+"-"+str(row[1].date.month)+"-"+str(row[1].date.day)
            anom_type = group_currently_processing+" unusual-daily"

            start_d = str(row[1].date - timedelta(days=40))
            end_d = str(row[1].date + timedelta(days=40))
            url = "https://my.smarking.net/rt/"+smarking_globals.garage_url.rstrip('\n')\
            +"/occupancy?occupancyType=regular&fromDateStr="+start_d+"&toDateStr="+end_d
                    
            temp_anomaly.append(mon)
            temp_anomaly.append(anom_type)
            temp_anomaly.append(url)
            temp_anomaly.append("No")
            temp_anomaly.append("No")
            
            smarking_globals.anomalies_for_google_docs.append(temp_anomaly)  
            
def gather_overnight_anomalies(group_now_processing, dates, indices):

    for row in dates[indices]:
        if ((str(row.date().year)+"-"+str(row.date().month)+"-"+str(row.date().day)) not in smarking_globals.holidays):
            found_dp = 0
            search_str = str(row.date().year)+"-"+str(row.date().month)+"-"+str(row.date().day)\
                            +group_now_processing
            if(search_str in smarking_globals.daily_peak_anomalies):
                found_dp = 1
                                    
            if(found_dp == 0):
                temp_anomaly=[]
                mon = str(row.date().year)+"-"+str(row.date().month)+"-"+str(row.date().day)

                anom_type = group_now_processing+" unusual-overnight"
                
                start_d = str(row.date() - timedelta(days=3))
                end_d = str(row.date() + timedelta(days=3))
                                #generate url
                url = "https://my.smarking.net/rt/"+smarking_globals.garage_url.rstrip('\n')\
                +"/occupancy?occupancyType=regular&fromDateStr="+start_d+"&toDateStr="+end_d
                    
                temp_anomaly.append(mon)
                temp_anomaly.append(anom_type)
                temp_anomaly.append(url)
                temp_anomaly.append("No")
                temp_anomaly.append("No")

                smarking_globals.anomalies_for_google_docs.append(temp_anomaly)    
                    
def gather_duration_anomalies(sum_one_hour, sum_ten_minutes, sum_t, group_now_processing):
    percent_one_hour = (sum_one_hour/float(sum_t))*100
    percent_ten_minute = (sum_ten_minutes/float(sum_t))*100
     
    if (percent_one_hour > 60.0):
        temp_anomaly=[]
        mon = str(smarking_globals.start_date_supplied)+" "+str(smarking_globals.end_date_supplied)
        anom_type = group_now_processing + str(percent_one_hour)+" % one hour parkers"
                    
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)

        smarking_globals.anomalies_for_google_docs.append(temp_anomaly)  
    if (percent_ten_minute > 5.0):
        temp_anomaly=[]
        mon = str(smarking_globals.start_date_supplied)+" "+str(smarking_globals.end_date_supplied)
        anom_type = group_now_processing+ str(percent_ten_minute)+" % ten minute parkers"
           
        url = "https://my.smarking.net/rt/"+smarking_globals.garage_url.rstrip('\n')\
        +"/duration-distribution?bucketInMinutes=10&fromDateStr="\
        +str(smarking_globals.start_date)+"&toDateStr="+str(smarking_globals.end_date)
                    
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)
        temp_anomaly.append(url)
        temp_anomaly.append("No")
        temp_anomaly.append("No")

        smarking_globals.anomalies_for_google_docs.append(temp_anomaly)