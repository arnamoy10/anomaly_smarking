#print "Importing libraries"
import os
import numpy as np
from pandas import bdate_range, DataFrame, date_range, to_datetime, Series

import requests
from datetime import datetime, timedelta
from dateutil import relativedelta
import rpy2.robjects as robjects


#twitter analysis for timeseries anomaly detection
from pyculiarity import detect_vec

import smarking_globals
import smarking_get_data
import smarking_gather_results


def get_zero_indices(training_data):
    indices = []  
        
    #training_data[garage_index][month_index]
    for ii in np.arange(0, len(training_data)):
        if (0 == training_data[ii]):
            indices.append(ii)
    return indices

def get_iqr_indices(training_data):
    
    indices = []
    
    #Algorithm (detecting non zero gaps):
    # If data points fall beyond 3 IQR -> REPORT gap
    p25 = np.percentile(training_data, 25)
    p75 = np.percentile(training_data, 75)
    iqr = np.subtract(*np.percentile(training_data, [75, 25]))

    #1.5 was too restrictive
    lower = p25 - 3 * (p75 - p25)
    upper = p75 + 3 * (p75 - p25)
    
    for m in np.arange(0,len(training_data)):
        if (round(training_data[m],2) != 0):
            if ((round(training_data[m],2) < round(lower,2)) or (round(training_data[m],2) > round(upper, 2))):
                indices.append(m)
    return indices

def get_change_indices(training_data):
    
    indices = []
    
    robjects.r("library(changepoint)")

    r_data = robjects.FloatVector(training_data)

    first_function = robjects.r['cpt.mean']
    second_function = robjects.r['cpts']

    result_1=first_function(r_data,method="BinSeg")
    result_2 = second_function(result_1)

    for ii in result_2:
        indices.append(int(ii))
    return indices

def calculate_mp_anomaly_indiv(months_max_occ, group_currently_processing):
    training_data = []

    training_data=list(map(float, months_max_occ))
            
    #Array to hold if there was an anomaly or not for a month
    #a value of 1 means zero anomaly and a value of 2 means unusual anomaly
    #we have one flag per month
    zero_indices = get_zero_indices(training_data) 
    iqr_indices =  get_iqr_indices(training_data) 
    change_indices = get_change_indices(training_data)
    
    #print ("change monthly",change_indices)
                    
    dates = date_range(smarking_globals.start_date, periods=smarking_globals.total_months+1, freq='M')
    
    #the last argument differentiate the type of anomaly
    smarking_gather_results.gather_anomalies_monthly_peak(dates,zero_indices, group_currently_processing,0)
    smarking_gather_results.gather_anomalies_monthly_peak(dates,iqr_indices, group_currently_processing,1)
    smarking_gather_results.gather_anomalies_monthly_peak(dates,change_indices, group_currently_processing,2)

def get_max_month_occupanies(group_occupancy):
    # contracts_occupancy[] and transients_occupancy[] looks like this:
    # [day_1_hour1, day1_hour2, ..., day365_hour24]
    
    month_occupancies=[]
    
    temp_date = smarking_globals.start_date
    month_end = smarking_globals.start_date
        
    #index to extract data from the master datastructures
    #e.g contracts and transients
    hour_index = 0
    month_index = 0
    while True:
        #calculate the month end date, so that we can extract data
        month_end = temp_date + relativedelta.relativedelta(day=31)
            
        if(month_end >= smarking_globals.end_date):
            #we are spilling over, get the rest
            days = (smarking_globals.end_date-temp_date).days + 1
            month_occupancies.append(np.amax(group_occupancy[hour_index:hour_index+days*24]))
            break
        else:
            #keep looping until we have found the end date
            days = (month_end-temp_date).days + 1
            month_occupancies.append(np.amax(group_occupancy[hour_index:hour_index+days*24])) 
                
            #update the hour index
            hour_index = hour_index + days*24 
            temp_date = month_end + timedelta(days=1)
    
    return month_occupancies

def calculate_monthly_peak_anomaly(group_occupancy, group_currently_processing):

    months_max_occ = get_max_month_occupanies(group_occupancy)
                       
    #Anomaly Detection 
    calculate_mp_anomaly_indiv(months_max_occ, group_currently_processing)

    
def get_daily_peak(training_data, ndays):
    daily_peak = []
    #get day index of the week Sunday -> 0, Monday -> 1 etc.
    day_index = smarking_globals.start_date.weekday()
    
    for jj in np.arange(0, ndays):
        day_index = day_index +1
        if(day_index == 7):
            day_index = 0
            continue
        if(day_index == 6):
            continue
        else:
            #we are at the other 5 days of the week, calculate the peak for a day and add
            lower = jj*24
            upper = lower+23
            daily_peak.append(np.amax(training_data[lower:upper]))
                                
    return daily_peak
                                            
def calculate_dp_anomaly(ndays, training_data, group_currently_processing): 
    #data_structure to hold daily peak occupancy of all garages
    #TODO as we are not doing multiple garages, this can be modified
    daily_peak = get_daily_peak(training_data, ndays)
                    
    #Anomaly Detection part
    
    #print daily_peak
    zero_indices = get_zero_indices(daily_peak)
    #print zero_indices
    iqr_indices =  get_iqr_indices(daily_peak) 
    change_indices = get_change_indices(daily_peak)
    
    #print ("change daily",change_indices)

    dates = bdate_range(smarking_globals.start_date, periods=ndays)
    
    #gather the anomalies in the master anomaly data structure
    #based on the anomalies reported
    smarking_gather_results.gather_anomalies_daily_peak(dates,zero_indices, group_currently_processing,0)
    smarking_gather_results.gather_anomalies_daily_peak(dates,iqr_indices, group_currently_processing,1)
    smarking_gather_results.gather_anomalies_daily_peak(dates,change_indices, group_currently_processing,2)


def get_daily(training_data, ndays):
    total_daily = []
        
    #get day index of the week Sunday -> 0, Monday -> 1 etc.
    day_index = smarking_globals.start_date.weekday()
        
    for nn in np.arange(0, ndays):
        day_index = day_index +1
        if(day_index == 7):
            day_index = 0
            continue
        if(day_index == 6):
            continue
        else:
                #we are at the other 5 days of the week, calculate the occupancy for a day and add
            lower = nn*24
            upper = lower+23

            total_daily.append(training_data[lower:upper+1])        
    return total_daily

def s_h_esd_algo(total_daily):
    #total_daily[garage_index][day_index][hour_index]
    #get the dates
    dates = bdate_range(smarking_globals.start_date, periods=len(total_daily))
    #print dates
    data=[]
    hours=[]
    index = 0
       
    #hard coding for one garage, sorry
    #ww is a day
    for ww in total_daily:              
        #create the hours for that day
        temp_hours= bdate_range(dates[index], periods=24,freq='H')
        m=0
        #ww is a day
        #for each hour
        for hr in ww:
            hours.append(temp_hours[m])
            data.append(hr)
            m=m+1
            #data.append(temp)
        index = index+1
                
    df1 = Series( (v for v in data) )
    
    try:
        results = detect_vec(df1, period = 120,
                             max_anoms=0.02,
                             direction='both')
    except RRuntimeError:
        #there is something wrong with the data, may be not periodic
        print ("could not run anomaly detection due to bad data")
        sys.exit(0)
    temp= results['anoms']

    indices=[]
    for index, row in temp.iterrows():
        indices.append(row['timestamp'])
    #now indices has all the indices of anomalies in the data.  
    #get the dates now
    result_dates=[]
    for ii in indices:
        result_dates.append(hours[int(ii)].date())
    return result_dates

def calculate_daily_indiv(ndays, training_data, group_currently_processing):  
    #data structure to hold the hourly data for the given days for
    #the garages
    total_daily = get_daily(training_data, ndays)         
 
    result_dates = s_h_esd_algo(total_daily)
                                                
    smarking_gather_results.gather_daily_anomaly(result_dates, group_currently_processing) 
    
def get_overnight(training_data, ndays):
    total_daily = []
    #get day index of the week Sunday -> 0, Monday -> 1 etc.
    day_index = smarking_globals.start_date.weekday()
        
    for nn in np.arange(0, ndays):
        day_index = day_index +1

        if(day_index == 7):
            day_index = 0
            continue
        if(day_index == 6):
            continue
        else:
            #we are at the other 5 days of the week, 
            #calculate the maximum night occupancy             
            lower_n = nn*24
            upper_n = lower_n+5
                    
            total_daily.append(np.max(training_data[lower_n:upper_n+1]))

    return total_daily
                   
def calculate_overnight_anomaly(ndays, training_data, group_now_processing):
    
    #preapre the dataset
    total_daily = get_overnight(training_data, ndays)
 
    #run anomaly detection and gather results in master
    #anomaly data structure
    indices = get_iqr_indices(total_daily)
    
    dates = bdate_range(smarking_globals.start_date, periods=ndays)
    smarking_gather_results.gather_overnight_anomalies(group_now_processing, dates, indices)
                    
def calculate_duration_anomalies(training_data, group_now_processing):
    #Heuristic:  We calculate the percentage of the number of parkers
    #present during the time 12AM-5AM
    #if there is a high spike in that, we have an anomaly
    #There can be two reasons for this
    #   1.  The whole day is flat so nighttime will be a high percentage (FP)
    #   2.  The whole day is not flat but still a spike in night occupancy
    #TODO:  Eliminate the days where peak daily occupancy is already reported.
    
    sum_t=np.sum(training_data)
    
    sum_one_hour = 0
    for iii in np.arange(0,6):
        sum_one_hour = sum_one_hour + training_data[iii]
        sum_ten_minutes = training_data[0]

    smarking_gather_results.gather_duration_anomalies(sum_one_hour, sum_ten_minutes, sum_t, group_now_processing)