# anomaly_smarking
Anomaly detection scripts at Smarking

RELEASE Version 1.1: Jan 30, 2017

Compatible with python 3

Steps:

   1. install R (https://cran.rstudio.com/bin/macosx/)
   
   2. Open an R console and install the "library(changepoint)"
        
        sudo R
        install.packages("library(changepoint)")
   
   3. use "pip install" to install the following libaries:
        
        pip3 install pyculiarity requests
   4. Modify the following file:
        
        cd /usr/local/lib/python3.6/site-packages/
        2to3 -w -n pyculiarity
        
   5. Change line 81 of /usr/local/lib/python3.6/site-packages/pyculiarity/detect_anoms.py to the following:
        
        'value': ps.to_numeric((decomp['trend'] + decomp['seasonal']).truncate())

   6. You will need to set the environment variable "Bearer". use the following command:

        export Bearer="blah" (you have to give the quotes also)**
     
   7. You can run the python script as follows
        
        python3 data_analysis_dynamic.py 860170 2017-01-01 2017-01-28
    
      arguments are <garage_id> <start_date> <end_date>

**The bearer can be found in the following way (google chrome):

1.  Open my.smarking.net
2.  Open developer tools
3.  Click on the "network" tab
4.  Hit refresh on the main window of my.smarking.net
5.  You will see a lot of lines appearing in the network tab of 
    developer tools
6.  Click on a line that looks like this:
    ?group=contract
7.  Then on the righ panel, you will see the "Response" tab open
8.  Click on the "Headers" tab in the right panel
9.  Look for a line
    Authorization:Bearer xxxxx
    
10. Voila, xxxxx is the value you have to set in your environment variable




PS- if you are planning to run analysis on a year beyond 2015-2017 (which somebody will do in future), you need to add a file called holidays\<year\>. e.g holidays2018 for holidays in 2018.  Please check the other holiday filed for the formatiing info.  

Please send an email to arnamoyb@ece.utoronto.ca if you have questions/ concerns.

Release History:

RELEASE Version 1.0: Jan 26, 2017
    
