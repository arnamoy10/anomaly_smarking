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

occupancies_all_groups = []
durations_all_groups = []
names_all_groups = []

contract_duration = []
transient_duration = []


#NEW: User type groups
user_type_groups = []

monthly_peak_zero = []


total_months = 0

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
    print ("Please set the Bearer environment variable")
    sys.exit(0)
bearer = "Bearer "+ str(os.environ['Bearer'])
headers = {"Authorization":bearer}
 
def set_garage_info_occupancy(con, tran):
    global contract_occupancy, transient_occupancy, garage_info_occupancy
    if ((con == 0) and (tran == 0)):
        garage_info_occupancy = 3
        print ("No Occupancy data present for this garage for the given time")
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
    global headers, contract_occupancy, transient_occupancy \
    , garage_info_occupancy, occupancies_all_groups, names_all_groups
    
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
        print ("No valid information received")
        sys.exit(0)
    
    #parse the JSON-formatted line
    for item in garage_info["value"]:
        #check if value contains anything
 
        group = str(item.get("group"))
        
        occupancies_all_groups.append(item.get("value"))
        names_all_groups.append(group)
        


    
    
def get_duration_data():
    
    global headers, contract_duration, transient_duration, garage_info_duration, garage_id, durations_all_groups
    
    #had to add 1 with the end _date because the midnight of the supplied end date goes to
    #end_date + 1
    url = "https://my.smarking.net/api/ds/v3/garages/"+garage_id+"/past/duration/between/"+start_date_supplied+"T00:00:00/"+str((to_datetime(end_date_supplied)+timedelta(1)).date())+"T00:00:00?bucketNumber=25&bucketInSeconds=600&gb=User+Type"

    #get the response using the url
    garage_info = get_json_info(url)
    
    #parse the JSON-formatted line
    for item in garage_info["value"]:   
        durations_all_groups.append(item.get("value"))
    
    #set_garage_info_duration(con_dur, tran_dur)    
    
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

def gather_anomalies_monthly(dates,indices, group_currently_processing,zero_iqr):
    '''
    global monthly_peak_anomalies_con, monthly_peak_anomalies_tran
    '''
    
    anomaly_dates = dates[indices]
    for anom_date in anomaly_dates:
        #create the anomaly and add to master list
        temp_anomaly=[]
        mon = str(anom_date.year)+"-"+str(anom_date.month)
        if(zero_iqr == 0):
            anom_type = "zero "+ group_currently_processing \
                        + " monthly peak"
            #also store in monthly_peak_zero for daily_peak_zero analysis
            monthly_peak_zero.append(mon)
        else:
            anom_type = "unusual "+ group_currently_processing \
                        + " monthly peak"
                    
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)

        anomalies_for_google_docs.append(temp_anomaly)
    
    #NEW: TODO
    #also add to data structure so that we can filter out
    #daily peak zero anomalies
    '''
    if(con_tran == 0): #contracts
        monthly_peak_anomalies_con.append("con-"+mon)
    else:
        monthly_peak_anomalies_tran.append("tran-"+mon)
    '''

def calculate_mp_anomaly_indiv(months_max_occ, group_currently_processing):
    training_data = []

    training_data=list(map(float, months_max_occ))
            
    #Array to hold if there was an anomaly or not for a month
    #a value of 1 means zero anomaly and a value of 2 means unusual anomaly
    #we have one flag per month
    zero_indices = get_zero_indices(training_data) 
    iqr_indices =  get_iqr_indices(training_data) 
                    
    dates = date_range(start_date, periods=total_months+1, freq='M')
    
    #the last argument differentiate the type of anomaly
    gather_anomalies_monthly(dates,zero_indices, group_currently_processing,0)
    gather_anomalies_monthly(dates,iqr_indices, group_currently_processing,1)

def get_max_month_occupanies(total_months, group_occupancy):
    # contracts_occupancy[] and transients_occupancy[] looks like this:
    # [day_1_hour1, day1_hour2, ..., day365_hour24]
        
    #TODO take out the garage_index by removing the loop
    #months_max_occ[month_index][garage_index][contract/transient]
    month_occupancies=[]
    
    temp_date = start_date
    month_end = start_date
        
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

def calculate_monthly_peak_anomaly(total_months, group_occupancy, group_currently_processing):
    #replace the following with one list
    #needed to do total_months+2 for various date perks
    months_max_occ = get_max_month_occupanies(total_months, group_occupancy)
                       
    #STAGE 3:  Anomaly Detection 
    
    calculate_mp_anomaly_indiv(months_max_occ, group_currently_processing)

    
def get_daily_peak(training_data, ndays):
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
            daily_peak.append(np.amax(training_data[lower:upper]))
                                
    return daily_peak
            
def gather_anomalies_daily(dates,indices, group_currently_processing,zero_iqr):
    
    
    anomaly_dates = dates[indices]

    for anom_date in anomaly_dates:
        temp_anomaly =[]
        if ((str(anom_date.year)+"-"+str(anom_date.month)+"-"+str(anom_date.day)) not in holidays):
            mon = str(anom_date.year)+"-"+str(anom_date.month)+"-"+str(anom_date.day)
            #print mon
            found = 0
            if(zero_iqr == 0):
                anom_type = "zero "+group_currently_processing+" daily-peak"
                if (str(anom_date.year)+"-"+str(anom_date.month)) in monthly_peak_zero:
                    #print "found in"
                    found = 1
            else:
                anom_type = "unusual "+group_currently_processing+" daily-peak"
            
            if (found == 0):
                temp_anomaly.append(mon)
                temp_anomaly.append(anom_type)
                anomalies_for_google_docs.append(temp_anomaly)
        
        #NEW TODO
        '''    
        #also add the date to the data structure
        if(con_tran == 0):
            daily_peak_anomalies_con.append(dates[ii].date())
        else:
            daily_peak_anomalies_tran.append(dates[ii].date())
        '''
                                    
def calculate_dp_anomaly(ndays, training_data, group_currently_processing): 
    #data_structure to hold daily peak occupancy of all garages
    #TODO as we are not doing multiple garages, this can be modified
    daily_peak = get_daily_peak(training_data, ndays)
                    
    #Anomaly Detection part
    
    #print daily_peak
    zero_indices = get_zero_indices(daily_peak)
    #print zero_indices
    iqr_indices =  get_iqr_indices(daily_peak) 

    dates = bdate_range(start_date, periods=ndays)
    
    #gather the anomalies in the master anomaly data structure
    #based on the anomalies reported
    gather_anomalies_daily(dates,zero_indices, group_currently_processing,0)
    gather_anomalies_daily(dates,iqr_indices, group_currently_processing,1)


def get_daily(training_data, ndays):
    total_daily = []
        
    #get day index of the week Sunday -> 0, Monday -> 1 etc.
    day_index = start_date.weekday()
        
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
    dates = bdate_range(start_date, periods=len(total_daily))
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
                                                
    df = DataFrame({'date': result_dates})
    df1=df.drop_duplicates('date')
                
    for row in df1.iterrows():
        if ((str(row[1].date.year)+"-"+str(row[1].date.month)+"-"+str(row[1].date.day)) not in holidays):
            temp_anomaly=[]
            mon = str(row[1].date.year)+"-"+str(row[1].date.month)+"-"+str(row[1].date.day)
            anom_type = group_currently_processing+" unusual-daily"

                
            temp_anomaly.append(mon)
            temp_anomaly.append(anom_type)
            anomalies_for_google_docs.append(temp_anomaly) 
    
    
def get_overnight(training_data, ndays):
    total_daily = []
    #get day index of the week Sunday -> 0, Monday -> 1 etc.
    day_index = start_date.weekday()

        
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
                
def gather_overnight_anomalies(group_now_processing, dates, indices):

    for row in dates[indices]:
        if ((str(row.date().year)+"-"+str(row.date().month)+"-"+str(row.date().day)) not in holidays):
            found_dp = 0
                                    
            if(found_dp == 0):
                temp_anomaly=[]
                mon = str(row.date().year)+"-"+str(row.date().month)+"-"+str(row.date().day)

                anom_type = group_now_processing+" unusual-overnight"
    
                temp_anomaly.append(mon)
                temp_anomaly.append(anom_type)

                anomalies_for_google_docs.append(temp_anomaly)    
def calculate_overnight_anomaly(ndays, training_data, group_now_processing):
    
    #preapre the dataset
    total_daily = get_overnight(training_data, ndays)
 
    #run anomaly detection and gather results in master
    #anomaly data structure
    indices = get_iqr_indices(total_daily)
    
    dates = bdate_range(start_date, periods=ndays)
    gather_overnight_anomalies(group_now_processing, dates, indices)
                    
    


def gather_duration_anomalies(sum_one_hour, sum_ten_minutes, sum_t, group_now_processing):
    percent_one_hour = (sum_one_hour/float(sum_t))*100
    percent_ten_minute = (sum_ten_minutes/float(sum_t))*100
     
    if (percent_one_hour > 60.0):
        temp_anomaly=[]
        mon = str(start_date_supplied)+" "+str(end_date_supplied)
        anom_type = group_now_processing + str(percent_one_hour)+" % one hour parkers"

                    
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)

        anomalies_for_google_docs.append(temp_anomaly)  
    if (percent_ten_minute > 5.0):
        temp_anomaly=[]
        mon = str(start_date_supplied)+" "+str(end_date_supplied)
        anom_type = group_now_processing+ str(percent_ten_minute)+" % ten minute parkers"

    
                    
        temp_anomaly.append(mon)
        temp_anomaly.append(anom_type)

        anomalies_for_google_docs.append(temp_anomaly)
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

    gather_duration_anomalies(sum_one_hour, sum_ten_minutes, sum_t, group_now_processing)

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
            print ("%s file not present, needed for analysis"%f_name)
            sys.exit(0)
        with open(f_name) as f:
            hdays= f.readlines()
        for mmm in hdays:
            dt = datetime.strptime(mmm.rstrip('\n'), date_format)
            holidays.append(str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)) 

def get_months():
    global total_months
    delta= relativedelta.relativedelta(start_date, end_date)
    months = delta.months
    years = delta.years


    total_months = abs(years)*12+abs(months)
def main():    
    
    #globals we are using
    global garage_id, garage_info_occupancy, anomalies_for_google_docs, start_date, end_date
        
    print ("  Processing Garage", garage_id) 
    
    try:
        start_date = datetime.strptime(start_date_supplied, date_format)
        end_date = datetime.strptime(end_date_supplied, date_format)
    except ValueError:
        print ("Incorrect date format, should be YYYY-mm-dd")
        sys.exit(0)

    #check if the from_date is < to_date
    if (start_date > end_date):
        print ("From_date can't be after to_date")
        sys.exit(0)
        
    #get the holidays
    get_holidays() 

    #STAGE 2: Getting the date
    get_occupancy_data()
    
    #print occupancies_all_groups
    #print names_all_groups
    get_duration_data()
    
    #if at least not two full months, skip the monthly analysis
    get_months()

    delta= end_date - start_date
    ndays = abs(delta.days)+1
    
    #print names_all_groups
    
    for ii in np.arange(0, len(occupancies_all_groups)):
        if (total_months > 6):
            calculate_monthly_peak_anomaly(total_months, occupancies_all_groups[ii], names_all_groups[ii])
        if (ndays > 20):
            calculate_dp_anomaly(ndays, occupancies_all_groups[ii], names_all_groups[ii]) 
        #we choose 18 because we need at least 
        #some data points to get signal properties
        if (ndays > 18):
            calculate_daily_indiv(ndays, occupancies_all_groups[ii], names_all_groups[ii])
        if (ndays > 18):
            calculate_overnight_anomaly(ndays, occupancies_all_groups[ii], names_all_groups[ii])
            
        calculate_duration_anomalies(occupancies_all_groups[ii], names_all_groups[ii])
        
    for item in anomalies_for_google_docs:
        print (item[0],item[1]) 
    #print len(anomalies_for_google_docs)
    #print anomalies_for_google_docs
    '''
                            
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

    '''

if __name__ == "__main__":   
    main()

