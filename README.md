Run the real time as follows

    	python3 real_time_checking_shesd.py


It needs the garage list file which has the list of garages to run real time analysis on.  It collects data every 10 minutes (implemented using threading.Timer) and checks for anomalies.  It create a log file anomaly_logs where it keeps metadata and prints out the detected anomalies in the terminal.

Also needs a "bearer" file with the authentication token.