#check if the master garage_name file exists

#data structure for holidays
global holidays
holidays = []

#where to store the anomalies
global anomalies_for_google_docs
anomalies_for_google_docs=[]


#get the number of hours, necessary to download occupancy
global date_format 
date_format = "%Y-%m-%d"

global occupancies_all_groups 
occupancies_all_groups= []
global durations_all_groups
durations_all_groups = []
global names_all_groups
names_all_groups = []

global monthly_peak_zero
monthly_peak_zero = []

global daily_peak_anomalies
daily_peak_anomalies = []

global total_months
total_months = 0


#the supplied start and end date
global start_date
global end_date

global bearer
global headers

# Get the garage_name, start and end dates
global garage_id 
global start_date_supplied 
global end_date_supplied 