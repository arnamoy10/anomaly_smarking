#check if the master garage_name file exists

#data structure for holidays
global holidays, anomalies_for_google_docs, date_format, occupancies_all_groups\
        , durations_all_groups, names_all_groups, monthly_peak_zero, daily_peak_anomalies\
        , total_months
holidays = []

#where to store the anomalies
anomalies_for_google_docs=[]

#get the number of hours, necessary to download occupancy
  
date_format = "%Y-%m-%d"

  
occupancies_all_groups= []
 
durations_all_groups = []
 
names_all_groups = []

monthly_peak_zero = []
 
daily_peak_anomalies = []
 
total_months = 0

#the supplied start and end date
global start_date, end_date, bearer, headers, garage_id ,start_date_supplied ,end_date_supplied 