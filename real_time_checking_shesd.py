import datetime, threading
import numpy as np
import requests
import json, os
import sys
from datetime import datetime, timedelta
from pandas import Series

#twitter analysis for timeseries anomaly detection
from pyculiarity import detect_vec

#get the garage names
garage_dict=[]
with open("garage_list") as f:
    for line in f:
        (key) = line.split()
        garage_dict.append(key)
        
#open log file
log_file = open("anomaly_log","a+")

anomaly_count_con=[0]*len(garage_dict)
anomaly_count_tran=[0]*len(garage_dict)

time  = 0
temp_values = []

#ndays in the hostory to look for pattern mismatch
days_window = 20

#change the authentication token accordingly
if(os.path.isfile('bearer') != True):
    print ("Please set the Bearer environment variable in a file called bearer")
    sys.exit(0)

#change the authentication token accordingly
with open("bearer") as f:
    bearer = "Bearer "+ str(f.readline().rstrip('\n'))

headers = {"Authorization":bearer}    


garage_info_occupancy=[0]*len(garage_dict)
anomalies=[]

def get_iqr_anomaly(training_data):
    indices  = []
    p25 = np.percentile(training_data, 25)
    p75 = np.percentile(training_data, 75)
    iqr = np.subtract(*np.percentile(training_data, [75, 25]))

    #1.5 was too restrictive
    lower = p25 - 3 * (p75 - p25)
    upper = p75 + 3 * (p75 - p25)
    
    for m in np.arange(0,len(training_data)):
        if ((round(training_data[m],2) < round(lower,2)) or (round(training_data[m],2) > round(upper, 2))):
            indices.append(m)  
    return indices
    


    
def check_error_real_time():
    global time
    global garage_info_occupancy
    global contracts
    global transients
    global anomalies
    
    
    #keep adding the values
    line_index = 0
    for i in garage_dict:
            
        contracts_hist = []
        contracts_real_time = []
        transients_hist = []
        transients_real_time = []  
            
        current_contract = 0.0
        current_transient = 0.0
            
        #print i
        con = 0
        tran = 0
        url="https://my.smarking.net/api/ds/v3/garages/"+str(i)+"/current/occupancy?gb=User+Type"
            #print url

        #get the response using the url
        response = requests.get(url,headers=headers)
        content = response.content
            
        #see if content was received.  If nothing  received, exit
        if (content == ""):
            #print "<p>No content received</p>"
            continue

        #we have collected all the data
        #each datapoint is for an hour in a given day
        try:
            garage_info = json.loads(content)
        except ValueError:
            #raise ValueError("No JSON Object received, please try again.")
            continue
    
    
        #print garage_info
    
        #parse the JSON-formatted line
        
        #if value not received for some reason, add 0 to value
        if "value" not in garage_info:
            #did not find anything, continue to next garage
            continue
        for item in garage_info["value"]:
            group = str(item.get("group"))
            if('Contract' in group):  
                current_contract = float(item.get("value"))
                con = 1
            if('Transient' in group):
                current_transient = float(item.get("value"))
                tran = 1
                    
        #days_window din age theke period length hobe ekhon koto ghonta tar upor
            
    
        # now prepare to get the historical data
        #getting the closest rounded off hour
        current_t= datetime.now()
            
        next_time = current_t+timedelta(hours=1)
            
        #skipping 11PM for now
        if ((current_t.hour == 23)or (current_t.hour == 0)):
            continue
        period_length = current_t.hour + 1
            
        #days_window din age theke ei period download
            
        # we have to make sure that we received all data correctly
        received_flag = 1
        for ii in reversed(np.arange(1,days_window+1)):
            day =  next_time-timedelta(days=ii)
            pred_url = "https://my.smarking.net/api/ds/v3/garages/"+str(i)+"/past/occupancy/from/"+str(day.year)+"-"+str(day.month)+"-"+str(day.day)+"T00:00:00/"+str(period_length)+"/1h?gb=User+Type"
#get the response using the url
            #print pred_url
            response = requests.get(pred_url,headers=headers)
            content = response.content
            
            #see if content was received.  If nothing  received, exit
            if (content == ""):
                #print "<p>No content received</p>"
                received_flag = 0
                break

            #we have collected all the data
            #each datapoint is for an hour in a given day
            try:
                garage_info = json.loads(content)
            except ValueError:
                #raise ValueError("No JSON Object received, please try again.")
                received_flag = 0
                break
            #parse the JSON-formatted line
        
            #if value not received for some reason, add 0 to value
            if "value" not in garage_info:
                #did not find anything, continue to next garage
                received_flag = 0
                break
                    
            con = 0
            tran = 0
            for item in garage_info["value"]:
                group = str(item.get("group"))
                #print "group ",group
                if('Contract' in group):  
                    for jj in item.get("value"):
                        contracts_hist.append(jj)
                    con = 1
                if('Transient' in group):
                    for jj in item.get("value"):
                        transients_hist.append(jj)
                    tran = 1
                
            if ((con == 0) and (tran == 0)):
                #print "did not receive contract and transient"
                received_flag = 0
                break
                    
            
        #if we did not receive all data correctly, go to a different garage
        if (received_flag == 0):
            continue
            
            
            
        #now contruct the data for now/ recent
        pred_url = "https://my.smarking.net/api/ds/v3/garages/"+str(i)+"/past/occupancy/from/"+str(current_t.year)+"-"+str(current_t.month)+"-"+str(current_t.day)+"T00:00:00/"+str(period_length-1)+"/1h?gb=User+Type"
#get the response using the url
        #print pred_url
        response = requests.get(pred_url,headers=headers)
        content = response.text
            
        #see if content was received.  If nothing  received, exit
        if (content == ""):
            #print "<p>No content received</p>"
            continue

        #we have collected all the data
        #each datapoint is for an hour in a given day
        try:
            garage_info = json.loads(content)
        except ValueError:
            #raise ValueError("No JSON Object received, please try again.")
            continue
    
        #parse the JSON-formatted line
        
        #if value not received for some reason, add 0 to value
        if "value" not in garage_info:
            #did not find anything, continue to next garage
            continue
        con = 0
        tran = 0
        for item in garage_info["value"]:
            group = str(item.get("group"))
            #print "group ",group
            if('Contract' in group):  
                for jj in item.get("value"):
                    contracts_real_time.append(jj)
                #finally append the real time data
                contracts_real_time.append(current_contract)
                con = 1
            if('Transient' in group):
                for jj in item.get("value"):
                    transients_real_time.append(jj)
                transients_real_time.append(current_transient)
                tran = 1  
        if ((con == 0) and (tran == 0)):
            #no data received
            continue
            
        #now form the training signal appending the history with the 
        #real time
        training_contract=[]
        training_transient=[]
            
            
        if (con == 1):
            for ii in contracts_hist:
                training_contract.append(ii)
            for ii in contracts_real_time:
                training_contract.append(ii)
            #detect anomalies and report
                
                
            log_file.write(str(i)+" running contract anomaly detection "+ str(datetime.now())+'\n')
            log_file.flush()
                
            #not enough data for signal processing in early hours
            indices=[]
                
            if ((current_t.hour == 1)or (current_t.hour == 2)
                or (current_t.hour == 3) or (current_t.hour == 4)
                or (current_t.hour == 5)):
                indices= get_iqr_anomaly(training_contract)
            else:
                df1 = Series( (v for v in training_contract) )
                try:
                    results = detect_vec(df1, period = period_length,
                                             max_anoms=0.02,
                                             direction='both')
                except RuntimeError:
                    #there is something wrong with the data, may be not periodic
                    #print "could not run detect_vec"
                    continue
                temp= results['anoms']
                for index, row in temp.iterrows():
                    indices.append(row['timestamp']) 
                
            anomalies = [mm for mm in indices if mm >=(period_length*(days_window+1) - 2)]
            if anomalies:
                #check how many times
                if(anomaly_count_con[line_index] == 5):
                    print (i,datetime.now(),anomaly_count_con[line_index], " Contract anomaly ", current_contract)
                    anomaly_count_con[line_index] = 0
                else:
                    anomaly_count_con[line_index] = anomaly_count_con[line_index] + 1
                    
        if (tran == 1):
            for ii in transients_hist:
                training_transient.append(ii)
            for ii in transients_real_time:
                training_transient.append(ii)

                
            log_file.write(str(i)+ " running transient anomaly detection "+str(datetime.now())+'\n')
            log_file.flush()
            indices=[]
                
            if ((current_t.hour == 1)or (current_t.hour == 2)
                   or (current_t.hour == 3) or (current_t.hour == 4)
                   or (current_t.hour == 5)):
                indices= get_iqr_anomaly(training_transient)
            else:
                df1 = Series( (v for v in training_transient) )
                try:
                     results = detect_vec(df1, period = period_length,
                                             max_anoms=0.02,
                                             direction='both')
                except RuntimeError:
                    #there is something wrong with the data, may be not periodic
                    #print "could not run detect_vec"
                    continue
                temp= results['anoms']
                for index, row in temp.iterrows():
                    indices.append(row['timestamp']) 
                        
            anomalies = [mm for mm in indices if mm >=(period_length*(days_window+1) - 2)]
            if anomalies:
                #check how many times
                if(anomaly_count_tran[line_index] == 5):
                    print (i,datetime.now(),anomaly_count_tran[line_index], " Transient anomaly ", current_transient)
                    anomaly_count_tran[line_index] = 0
                else:
                    anomaly_count_tran[line_index] = anomaly_count_tran[line_index] + 1 
                #else:
                #    print "no anomalies"
                
                
        line_index = line_index + 1
    threading.Timer(600, check_error_real_time).start()
    
check_error_real_time()