# anomaly_smarking
Anomaly detection scripts at Smarking

RELEASE Version 1.0: Jan 26, 2017

Required Python Libraries:

   use "pip install" to install the following libaries:
       
       1. pyculiarity (library for signal processing anomaly detection)
       2. pandas
       3. numpy
       4. json
       5. requests
       6. datetime
       7. dateutil

Note: The package pyculiarity currently requires rpy2 in order to use R's stl function, so a working installation of R is necessary.
       
After you have installed the above libraries, You will need to set the environment variable "Bearer".

To set it, use the following command:

     export Bearer="blah" (you have to give the quotes also)
     
The bearer can be found in the following way (google chrome):

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
    
