import sys

if(len(sys.argv) < 4):
    print "Usage: python data_analysis.py <garage_id> <start_date> <end_date>"
    print "start and end date should be in YYYY-mm-dd format"
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

#STAGE 1: Perform some preprocessing before running main()

# Get the garage_name, start and end dates
garage_id = sys.argv[1]
start_date_supplied = sys.argv[2]
end_date_supplied = sys.argv[3]

#check if the master garage_name file exists

#data structure for holidays
holidays = []

#where to store the anomalies
anomalies_for_google_docs=[]


#get the number of hours, necessary to download occupancy
date_format = "%Y-%m-%d"

    
#Array to hold if there was data for occupancy present at the garage 
# 0 -> everything present
# 1 -> contract present
# 2 -> transient present
# 3 -> nothing present                  
garage_info_occupancy = 0
garage_info_duration = 0

line_index=0

#objects to store the parsed result
contract_occupancy = []
transient_occupancy = []

contract_duration = []
transient_duration = []

# data structure to filter out daily zero peak anomalies
# because if a month has zero peak anomaly, you do not
# need to report daily zero peaks
monthly_peak_anomalies_con =[]
monthly_peak_anomalies_tran =[]


#data structure to filter out overnight anomalies
#from daily peak anomalies
daily_peak_anomalies_con = []
daily_peak_anomalies_tran= []


#the supplied start and end date
start_date=datetime.now()
end_date=datetime.now()

#We do one garage at a time so, just one iteration is fine
#TODO: take out the loop and fix indentation

#change the authentication token accordingly
if ('Bearer' not in os.environ):
    print "Please set the Bearer environment variable"
    sys.exit(0)
bearer = "Bearer "+ str(os.environ['Bearer'])
headers = {"Authorization":bearer}
 
def set_garage_info_occupancy(con, tran):
    global contract_occupancy, transient_occupancy, garage_info_occupancy
    if ((con == 0) and (tran == 0)):
        garage_info_occupancy = 3
        print "No Occupancy data present for this garage for the given time"
        return
    if (con == 0):
        l = len(transient_occupancy)
        contract_occupancy = [0] * l
        garage_info_occupancy = 2
    if (tran == 0):
        l = len(contract_occupancy)
        transient_occupancy = [0] * l
        garage_info_occupancy = 1

def get_json_info(url):
    #get the response using the url
    response = requests.get(url,headers=headers)
    content = response.content

    #see if content was received.  If nothing  received, exit
    if (content == ""):
        print "No content received"
        sys.exit(0)

    #we have collected all the data
    #each datapoint is for an hour in a given day
    try:
        garage_info = json.loads(content)
    except ValueError:
        print "No JSON Object received for occupancy, please try again."
        sys.exit(0)
    return garage_info


def get_occupancy_data():
    global headers, contract_occupancy, transient_occupancy, garage_info_occupancy
    
    #get the duration for the supplied date range so that we can create the URL

    delta= end_date - start_date

    duration_hours = 0
    if delta.days == 0:
        duration_hours = 24
    else:
        duration_hours = (abs(delta.days)+1) * 24
    
    url="https://my.smarking.net/api/ds/v3/garages/"+str(garage_id)+"/past/occupancy/from/"+start_date_supplied+"T00:00:00/"+str(duration_hours)+"/1h?gb=User+Type"
    
    con = 0
    tran = 0

    garage_info = get_json_info(url)
    
    if 'value' not in garage_info:
        print "No valid information received"
        sys.exit(0)
    
    #parse the JSON-formatted line
    for item in garage_info["value"]:
        #check if value contains anything
 
        group = str(item.get("group"))
        if('Contract' in group):    
            contract_occupancy = item.get("value")
            con = 1
        if('Transient' in group):
            transient_occupancy = item.get("value")
            tran = 1
    
    set_garage_info_occupancy(con,tran)
        


def set_garage_info_duration(con_dur, tran_dur): 
    global contract_duration, transient_duration, garage_info_duration
    if ((con_dur == 0) and (tran_dur == 0)):
        garage_info_duration = 3
        print "No duration data for this garage"
        return
    if (con_dur == 0):
        l = len(transient_duration)
        contract_duration = [0] * l
        garage_info_duration = 2
    if (tran_dur == 0):
        l = len(contract_duration)
        transient_duration = [0] * l
        garage_info_duration = 1 
    
    
def get_duration_data():
    
    global headers, contract_duration, transient_duration, garage_info_duration, garage_id
    
    #had to add 1 with the end _date because the midnight of the supplied end date goes to
    #end_date + 1
    url = "https://my.smarking.net/api/ds/v3/garages/"+garage_id+"/past/duration/between/"+start_date_supplied+"T00:00:00/"+str((to_datetime(end_date_supplied)+timedelta(1)).date())+"T00:00:00?bucketNumber=25&bucketInSeconds=600&gb=User+Type"
    
    

    #get the response using the url
    garage_info = get_json_info(url)
      
    con_dur = 0
    tran_dur = 0
    
    #parse the JSON-formatted line
    for item in garage_info["value"]:
        group = str(item.get("group"))
        if('Contract' in group):    
            contract_duration = item.get("value")
            con_dur = 1
        if('Transient' in group):
            transient_duration = item.get("value")
            tran_dur = 1
    

    set_garage_info_duration(con_dur, tran_dur)           

def get_zero_iqr_monthly(training_data):
    anomaly_present = [0] * len(training_data[0])
        
        
    #training_data[garage_index][month_index]
    for ii in np.arange(0, len(training_data)):
        for m in np.arange(0,len(training_data[ii])):
            if (0 == training_data[ii][m]):
                anomaly_present[m] = 1
        
    #Algorithm (detecting non zero gaps):
    # If data points fall beyond 3 IQR -> REPORT gap

    for ii in np.arange(0, len(training_data)):
        p25 = np.percentile(training_data[ii], 25)
        p75 = np.percentile(training_data[ii], 75)
        iqr = np.subtract(*np.percentile(training_data[ii], [75, 25]))

        #1.5 was too restrictive
        lower = p25 - 3 * (p75 - p25)
        upper = p75 + 3 * (p75 - p25)
    
        for m in np.arange(0,len(training_data[ii])):
            if ((round(training_data[ii][m],2) < round(lower,2)) or (round(training_data[ii][m],2) > round(upper, 2))):
                anomaly_present[m] = 2  
        return anomaly_present

def gather_zero_anomalies_monthly(ii,dates,con_tran):
    global monthly_peak_anomalies_con, monthly_peak_anomalies_tran
    #create the anomaly and add to master list
    temp_anomaly=[]
    mon = str(dates[ii].year)+"-"+str(dates[ii].month)
    if(con_tran == 0): #contracts
        anom_type = "contract-zero-monthly-peak"
    else:
        anom_type = "transient-zero-monthly-peak"
                    
    temp_anomaly.append(mon)
    temp_anomaly.append(anom_type)

    anomalies_for_google_docs.append(temp_anomaly)
                    
    #also add to data structure so that we can filter out
    #daily peak zero anomalies
    if(con_tran == 0): #contracts
        monthly_peak_anomalies_con.append("con-"+mon)
    else:
        monthly_peak_anomalies_tran.append("tran-"+mon)

def gather_unusual_anomalies_monthly(ii,dates,con_tran):
    temp_anomaly=[]
    mon = str(dates[ii].year)+"-"+str(dates[ii].month)
    if (con_tran == 0):
        anom_type = "contract-unusual-monthly-peak"
    else:
        anom_type = "transient-unusual-monthly-peak"
                    
    temp_anomaly.append(mon)
    temp_anomaly.append(anom_type)
    anomalies_for_google_docs.append(temp_anomaly)  

def calculate_mp_anomaly_indiv(con_tran, total_months, months_max_occ):
    training_data = []
    #forming the training data set
    #TODO take out the loop
    for ii in np.arange(0, 1):
        t = []                                      
        for jj in np.arange(0, total_months+1):
            if (con_tran == 0): #contracts
                t.append(months_max_occ[jj][ii][0])
            else:
                t.append(months_max_occ[jj][ii][1])

        t1=map(float, t)
        training_data.append(t1)
            
    #Array to hold if there was an anomaly or not for a month
    #a value of 1 means zero anomaly and a value of 2 means unusual anomaly
    #we have one flag per month
    anomaly_present = get_zero_iqr_monthly(training_data)     
                
    dates = date_range(start_date, periods=total_months+1, freq='M')
    
    
    for ii in np.arange(0,len(anomaly_present)):
        if anomaly_present[ii] == 1:
            gather_zero_anomalies_monthly(ii,dates,con_tran)
                        
        if anomaly_present[ii] == 2:
            gather_unusual_anomalies_monthly(ii, dates, con_tran)  

def get_max_month_occupanies(total_months):
    # contracts_occupancy[] and transients_occupancy[] looks like this:
    # [day_1_hour1, day1_hour2, ..., day365_hour24]
        
    #TODO take out the garage_index by removing the loop
    #months_max_occ[month_index][garage_index][contract/transient]
    months_max_occ=[]
    month_occupancies=[[] for ii in range(total_months+2)]
    
    #TODO take out the loop
    for ii in np.arange(0,1):

        temp_date = to_datetime(start_date_supplied)
        month_end = to_datetime(start_date)
        
        #index to extract data from the master datastructures
        #e.g contracts and transients
        hour_index = 0
        month_index = 0
        while True:
            #calculate the month end date, so that we can extract data
            month_end = temp_date + relativedelta.relativedelta(day=31)
            
            if(month_end >= end_date):
                #we are spilling over, get the rest
                days = (end_date-temp_date).days + 1
                #TODO take out the following check
                if (garage_info_occupancy == 3):
                    return
                else:
                    l = []
                    #print hour_index, days, hour_index+days*24-1
                    l.append(np.amax(contract_occupancy[hour_index:hour_index+days*24]))
                    l.append(np.amax(transient_occupancy[hour_index:hour_index+days*24]))
                    month_occupancies[month_index].append(l)                    
                break
            else:
                #keep looping until we have found the end date
                days = (month_end-temp_date).days + 1
                #TODO take out the following check
                if (garage_info_occupancy == 3):
                    return
                else:
                    l = []
                    #print hour_index, days, hour_index+days*24-1
                    l.append(np.amax(contract_occupancy[hour_index:hour_index+days*24]))
                    l.append(np.amax(transient_occupancy[hour_index:hour_index+days*24]))
                    month_occupancies[month_index].append(l)  
                
                #update the hour index
                hour_index = hour_index + days*24 
                temp_date = month_end + timedelta(days=1)
            month_index = month_index + 1
        for jj in np.arange(0, month_index+1):
            months_max_occ.append(month_occupancies[jj])
        return months_max_occ

def calculate_monthly_peak_anomaly(total_months):
    #replace the following with one list
    #needed to do total_months+2 for various date perks
    months_max_occ = get_max_month_occupanies(total_months)
            
                
    #STAGE 3:  Anomaly Detection     
                                           
    #dealing with contracts
    if((garage_info_occupancy == 0) or (garage_info_occupancy == 1)):
        calculate_mp_anomaly_indiv(0, total_months, months_max_occ)
        
    #Do the same thing for Transients               
                                           
    if((garage_info_occupancy == 0) or (garage_info_occupancy == 2)):
        calculate_mp_anomaly_indiv(1, total_months, months_max_occ)
    

    
def get_daily_peak(con_tran, ndays):
    #data_structure for a single garage
    temp_daily_peak = []
    daily_peak = []
    #get day index of the week Sunday -> 0, Monday -> 1 etc.
    day_index = start_date.weekday()
    
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
            if(con_tran == 0):
                temp_daily_peak.append(np.amax(contract_occupancy[lower:upper]))
            else:
                temp_daily_peak.append(np.amax(transient_occupancy[lower:upper]))
                                
    #add the daily peak for the garage of the date range to 
    #the master list
    daily_peak.append(temp_daily_peak)
    return daily_peak
            
def detect_zero_iqr(data):
    anomaly_present = [0] * len(data[0])

    #Algorithm 1:  If there are 0 in weekdays, there may be
    # something wrong
    for ii in np.arange(0,len(data)):
        for jj in np.arange(0,len(data[ii])):
            if(data[ii][jj] == 0):
                anomaly_present[jj] = 1
        
    #Algorithm 2:  Check for non zero gaps, like previous.
    for ii in np.arange(0,len(data)):
        #if ( anomaly_present[i] == 0):
        p25 = np.percentile(data[ii], 25)
        p75 = np.percentile(data[ii], 75)
        iqr = np.subtract(*np.percentile(data[ii], [75, 25]))

        #1.5 was too restrictive
        lower = p25 - 3 * (p75 - p25)
        upper = p75 + 3 * (p75 - p25)
    
        for m in np.arange(0,len(data[ii])):
            if ((round(data[ii][m],2) < round(lower,2)) or (round(data[ii][m],2) > round(upper, 2))):        
                anomaly_present[m] = 2
                    
    return anomaly_present
            
def gather_zero_anomalies(ii,dates,con_tran):
    if ((str(dates[ii].year)+"-"+str(dates[ii].month)+"-"+str(dates[ii].day)) not in holidays):
        temp_anomaly=[]
        mon = str(dates[ii].year)+"-"+str(dates[ii].month)+"-"+str(dates[ii].day)
        if(con_tran == 0):
            anom_type = "contract-zero-daily-peak"
        else:
            anom_type = "transient-zero-daily-peak"
                                
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)
                            
        found = 0
                            
        if (con_tran == 0):
            check_string = "con-"+str(dates[ii].year)+"-"+str(dates[ii].month)
            if (check_string in monthly_peak_anomalies_con):
                found = 1
        else:
            check_string = "tran-"+str(dates[ii].year)+"-"+str(dates[ii].month)
            if (check_string in monthly_peak_anomalies_tran):
                found = 1

        if (found == 0):
            #monthly peak zero was not reported
            anomalies_for_google_docs.append(temp_anomaly)
                        
            #also add the date to the data structure
            if(con_tran == 0):
                daily_peak_anomalies_con.append(dates[ii].date())
            else:
                daily_peak_anomalies_tran.append(dates[ii].date())
def gather_unusual_anomalies(ii,dates, con_tran):
    if ((str(dates[ii].year)+"-"+str(dates[ii].month)+"-"+str(dates[ii].day)) not in holidays):
        temp_anomaly=[]
        mon = str(dates[ii].year)+"-"+str(dates[ii].month)+"-"+str(dates[ii].day)
        if(con_tran == 0):
            anom_type = "contract-unusual-daily-peak"
        else:
            anom_type = "transient-unusual-daily-peak"
                            
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)
        anomalies_for_google_docs.append(temp_anomaly)
                        
        #also add the date to the data structure
        if(con_tran == 0):
            daily_peak_anomalies_con.append(dates[ii].date())
        else:
            daily_peak_anomalies_tran.append(dates[ii].date())
                                    
def calculate_dp_anomaly_indiv(con_tran, ndays): 
    start_date = to_datetime(start_date_supplied)
    end_date = to_datetime(end_date_supplied)
    #data_structure to hold daily peak occupancy of all garages
    #TODO as we are not doing multiple garages, this can be modified
    daily_peak = get_daily_peak(con_tran, ndays)
                    
    #Anomaly Detection part

    #hard coded for dealing with just one garage
    anomaly_present = detect_zero_iqr(daily_peak)

    dates = bdate_range(start_date, periods=ndays)
                
    
    for ii in np.arange(0,len(anomaly_present)):
            
        #Zero Gap anomaly
        if anomaly_present[ii] == 1:
            gather_zero_anomalies(ii,dates,con_tran)
                        
        #Gap anomaly        
        if anomaly_present[ii] == 2:
            gather_unusual_anomalies(ii,dates,con_tran)
    
def calculate_daily_peak_anomaly(ndays):
    #TODO take out the following loop
    for ii in np.arange(0, 1):
        if((garage_info_occupancy == 0) or (garage_info_occupancy == 1)):
            calculate_dp_anomaly_indiv(0, ndays)
    #Do the same thing for "transients"
    for ii in np.arange(0, 1):
        if((garage_info_occupancy == 0) or (garage_info_occupancy == 2)):
            calculate_dp_anomaly_indiv(1, ndays)
    


def get_daily(con_tran, ndays):
    total_daily = []
    #TODO make start_date global
    start_date = to_datetime(start_date_supplied)
    end_date = to_datetime(end_date_supplied)
        
    #get day index of the week Sunday -> 0, Monday -> 1 etc.
    day_index = start_date.weekday()
        
    for mm in np.arange(0, 1):
        #TODO take out the next check
            
        #data_structure for a single garage
        temp_daily = []
        
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
            
                if(con_tran == 0):
                    temp_daily.append(contract_occupancy[lower:upper+1])
                else:
                    temp_daily.append(transient_occupancy[lower:upper+1])
        
        total_daily.append(temp_daily) 
    return total_daily

def s_h_esd_algo(total_daily):
    #total_daily[garage_index][day_index][hour_index]
    #get the dates
    dates = bdate_range(to_datetime(start_date_supplied)
                        , periods=len(total_daily[0]))
    #print dates
    data=[]
    hours=[]
    index = 0
       
    #hard coding for one garage, sorry
    #ww is a day
    for ww in total_daily[0]:
                   
        #create the hours for that day
        temp_hours= bdate_range(dates[index], periods=len(ww),freq='H')
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
                
    results = detect_vec(df1, period = 120,
                max_anoms=0.02,
                direction='both')
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
def calculate_daily_indiv(con_tran, ndays):
    
    #data structure to hold the hourly data for the given days for
    #the garages
    total_daily = get_daily(con_tran, ndays)

             
    for ss in np.arange(0, len(total_daily)):

        result_dates = s_h_esd_algo(total_daily)
                    
                                    
        df = DataFrame({'date': result_dates})
        df1=df.drop_duplicates('date')
                
        for row in df1.iterrows():
            if ((str(row[1].date.year)+"-"+str(row[1].date.month)+"-"+str(row[1].date.day)) not in holidays):
                temp_anomaly=[]
                mon = str(row[1].date.year)+"-"+str(row[1].date.month)+"-"+str(row[1].date.day)
                if(con_tran == 0):
                    anom_type = "contract-unusual-daily"
                else:
                    anom_type = "transient-unusual-daily"
                                
                            
                    
                temp_anomaly.append(mon)
                temp_anomaly.append(anom_type)
                anomalies_for_google_docs.append(temp_anomaly) 
    
def calculate_daily_anomaly(ndays):
    
    #only populate data for the weekdays and calculate anomalies
    #TODO take out the next loop
    if((garage_info_occupancy == 0) or (garage_info_occupancy == 1)):
        calculate_daily_indiv(0, ndays)
    
    if((garage_info_occupancy == 0) or (garage_info_occupancy == 2)):  
        calculate_daily_indiv(1, ndays)
    



def get_overnight(con_tran, ndays):
    total_daily = []
    start_date = to_datetime(start_date_supplied)
    #get day index of the week Sunday -> 0, Monday -> 1 etc.
    day_index = start_date.weekday()
    #data_structure for a single garage
    temp_daily = []
        
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
                    
            lower = nn*24
            upper = lower + 23
                    
            if(con_tran == 0):
                temp_daily.append(np.max(contract_occupancy[lower_n:upper_n+1]))
            else:
                temp_daily.append(np.max(transient_occupancy[lower_n:upper_n+1]))
        
    total_daily.append(temp_daily)  
    return total_daily

def get_iqr_indices_overnight(total_daily):
    #total_daily[garage_index][day_index][hour_index]
    data=[]
    hours=[]
 
    index = 0
    
    #hard coding for one garage, sorry
    #ww is a day
    for ww in total_daily[0]:
        data.append(ww)
                    

    p25 = np.percentile(data, 25)
    p75 = np.percentile(data, 75)
    iqr = np.subtract(*np.percentile(data, [75, 25]))

    #1.5 was too restrictive
    lower = p25 - 3 * (p75 - p25)
    upper = p75 + 3 * (p75 - p25)
    
    indices = []
    for m in np.arange(0,len(data)):
        if ((round(data[m],2) < round(lower,2)) or (round(data[m],2) > round(upper, 2))): 
            indices.append(m)
    return indices
                
def gather_overnight_anomalies(con_tran, total_daily,indices):
    dates = bdate_range(start_date, periods=len(total_daily[0]))

    for row in dates[indices]:
        if ((str(row.date().year)+"-"+str(row.date().month)+"-"+str(row.date().day)) not in holidays):
            found_dp = 0
            if (con_tran == 0):
                if(row.date() in daily_peak_anomalies_con):
                    found_dp = 1
            elif (con_tran ==1):
                if(row.date() in daily_peak_anomalies_tran):
                    found_dp = 1
                                    
            if(found_dp == 0):
                temp_anomaly=[]
                mon = str(row.date().year)+"-"+str(row.date().month)+"-"+str(row.date().day)
                if (con_tran == 0):
                    anom_type = "contract-unusual-overnight"
                else:
                    anom_type = "transient-unusual-overnight"
                                
                    
                temp_anomaly.append(mon)
                temp_anomaly.append(anom_type)

                anomalies_for_google_docs.append(temp_anomaly)    
def calculate_overnight_indiv(con_tran, ndays):
    
    total_daily = get_overnight(con_tran, ndays)
 
    for ss in np.arange(0, len(total_daily)):
        indices = get_iqr_indices_overnight(total_daily)
        gather_overnight_anomalies(con_tran, total_daily, indices)
                    
    
def calculate_overnight_anomaly(ndays):
    
    #only populate data for the weekdays
    #TODO take out the following loop
    for mm in np.arange(0, 1):
        if((garage_info_occupancy == 0) or (garage_info_occupancy == 1)):
            calculate_overnight_indiv(0, ndays)


    #for transients
    
    #TODO: take out the following loop
    for kk in np.arange(0, 1):
        if((garage_info_occupancy == 0) or (garage_info_occupancy == 2)):
            calculate_overnight_indiv(1, ndays)


def gather_duration_anomalies(sum_one_hour, sum_ten_minutes, sum_t):
    percent_one_hour = (sum_one_hour/float(sum_t))*100
    percent_ten_minute = (sum_ten_minutes/float(sum_t))*100
     
    if (percent_one_hour > 60.0):
        temp_anomaly=[]
        mon = str(start_date_supplied)+" "+str(end_date_supplied)
        anom_type = str(percent_one_hour)+" % one hour parkers"

                    
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)

        anomalies_for_google_docs.append(temp_anomaly)  
    if (percent_ten_minute > 5.0):
        temp_anomaly=[]
        mon = str(start_date_supplied)+" "+str(end_date_supplied)
        anom_type = str(percent_ten_minute)+" % ten minute parkers"

    
                    
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)

        anomalies_for_google_docs.append(temp_anomaly)
def calculate_duration_anomalies():
    
    if (garage_info_duration == 3):
        return
    #Heuristic:  We calculate the percentage of the number of parkers
    #present during the time 12AM-5AM
    #if there is a high spike in that, we have an anomaly
    #There can be two reasons for this
    #   1.  The whole day is flat so nighttime will be a high percentage (FP)
    #   2.  The whole day is not flat but still a spike in night occupancy
    #TODO:  Eliminate the days where peak daily occupancy is already reported.
    
    if (garage_info_duration == 0):
        sum_t=np.sum(contract_duration)+np.sum(transient_duration)
    
        sum_one_hour = 0
        for iii in np.arange(0,6):
            sum_one_hour = sum_one_hour + contract_duration[iii] + transient_duration[iii]
        
        sum_ten_minutes = contract_duration[0] + transient_duration[0]
    elif (garage_info_duration == 1):
        sum_t=np.sum(contract_duration)
    
        sum_one_hour = 0
        for iii in np.arange(0,6):
            sum_one_hour = sum_one_hour + contract_duration[iii] 
        
        sum_ten_minutes = contract_duration[0] 
    else:
        sum_t=np.sum(transient_duration)
    
        sum_one_hour = 0
        for iii in np.arange(0,6):
            sum_one_hour = sum_one_hour + transient_duration[iii]
        
        sum_ten_minutes = transient_duration[0]
    gather_duration_anomalies(sum_one_hour, sum_ten_minutes, sum_t)

def get_holidays():
    years_t=[]
    start_year = start_date.year
    end_year = end_date.year
            
    if (start_year == end_year):
        years_t.append(start_year)
    else:
        years_t.append(start_year)
        years_t.append(end_year)
    for year_t in years_t:
        #filename
        f_name = "holidays"+str(year_t)
        if(os.path.isfile(f_name) != True):
            print "%s file not present, needed for analysis"%f_name
            sys.exit(0)
        with open(f_name) as f:
            hdays= f.readlines()
        for mmm in hdays:
            dt = datetime.strptime(mmm.rstrip('\n'), date_format)
            holidays.append(str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)) 

def get_months():
    delta= relativedelta.relativedelta(start_date, end_date)
    months = delta.months
    years = delta.years


    total_months = abs(years)*12+abs(months)
    return total_months
def main():    
    
    #globals we are using
    global garage_id, garage_info_occupancy, anomalies_for_google_docs, start_date, end_date
        
    print "  Processing Garage", garage_id
    sys.stdout.flush()    
    
    try:
        start_date = datetime.strptime(start_date_supplied, date_format)
        end_date = datetime.strptime(end_date_supplied, date_format)
    except ValueError:
        print "Incorrect date format, should be YYYY-mm-dd"
        sys.exit(0)

    #check if the from_date is < to_date
    if (start_date > end_date):
        print "From_date can't be after to_date"
        sys.exit(0)
        
    #get the holidays
    get_holidays() 

    #STAGE 2: Getting the date
    get_occupancy_data()
    get_duration_data()
    
    #if at least not two full months, skip the monthly analysis
    total_months = get_months()

    delta= end_date - start_date
    ndays = abs(delta.days)+1
    
    if (total_months > 6):
        calculate_monthly_peak_anomaly(total_months)
                            
    #######################################
    #Daily PEAK Occupancy                      #
    #######################################
    
    if (ndays > 20):
        calculate_daily_peak_anomaly(ndays)                 
    
    #######################################
    #Daily Occupancy                      #
    #######################################
    #we choose 18 because we need at least 
    #some data points to get signal properties
    if (ndays > 18):
        calculate_daily_anomaly(ndays)
    
    ############################
    #   Overnight Occupancy    #
    ############################
    
    if (ndays > 18):
        calculate_overnight_anomaly(ndays)
    
    #####################
    #duration anomalies #
    #####################    
         
    calculate_duration_anomalies()
    for item in anomalies_for_google_docs:
        print item[0],"-",item[1]



if __name__ == "__main__":   
    main()

