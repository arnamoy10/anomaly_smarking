import smarking_globals
import sys, os
from datetime import datetime, timedelta
from dateutil import relativedelta

def check():
    if smarking_globals.garage_id == "":
        print ("<p>Please give a valid garage ID</p>")
        sys.stdout.flush()
        sys.exit(0)

    #change the authentication token accordingly
    if(os.path.isfile('bearer') != True):
        print ("<p>Please set the Bearer environment variable in a file called bearer</p>")
        sys.exit(0)
    
    #get the garage names

    #check if the master garage_name file exists
    if(os.path.isfile('garage_names') != True):
        print ("<pstyle='color:darkcyan;font-size: 16px;'>  The garage names list was not found.  Make sure to create one using the add garage link.  </p>")
        sys.exit(0)

    with open("garage_names") as f:
        for line in f:
            (key, url, name) = line.split(",")
            smarking_globals.garage_name_dict[int(key)] = name
            smarking_globals.garage_url_dict[int(key)] = url
        
    #check if the supplied garage exits in the master list or not
    if int(smarking_globals.garage_id.rstrip('\n')) not in smarking_globals.garage_name_dict.keys():
        print ("<pstyle='color:darkcyan;font-size: 20px;'>  The garage has not been added yet.  Make sure to add it  using the add garage link.  </p>")
        sys.exit(0)
    
    smarking_globals.garage_name=smarking_globals.garage_name_dict[int(smarking_globals.garage_id)]
    smarking_globals.garage_url=smarking_globals.garage_url_dict[int(smarking_globals.garage_id)]
    
    try:
        smarking_globals.start_date = datetime.strptime(smarking_globals.start_date_supplied, smarking_globals.date_format)
        smarking_globals.end_date = datetime.strptime(smarking_globals.end_date_supplied, smarking_globals.date_format)
    except ValueError:
        print ("<p style='color:darkcyan;font-size: 22px;'>Incorrect date format, should be YYYY-mm-dd</p>")
        sys.stdout.flush() 
        sys.exit(0)

    #check if the from_date is < to_date
    if (smarking_globals.start_date > smarking_globals.end_date):
        print ("<p style='color:darkcyan;font-size: 22px;'>From_date can't be after to_date<p>")
        sys.stdout.flush() 
        sys.exit(0)
    
    
