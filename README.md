This project includes a collection of Python scripts to download Purple Air
air quality (particulate matter) data from the thingspeak.com API for a set of sensors that are
within a specified geographic region (minimum/maximum latitude/longitude). 
The scripts include methods to download data from the thingspeak API, merge 
and augment the data and then perform various spatial and temporal analyses 
along with visualizations to be able to comprehend air quality across space and time.  

For now, this repository includes only the download scripts as I am still
working on the analytic elements.    

# Extract, clean and merge data

## extract_all_purple_air_sensors.py

This script reads from the master list of Purple Air Sensors 
JSON file (https://www.purpleair.com/json) to identify all 
the sensors that have been registered to update the local 
sqlite database.

For each sensor, it checks whether it has seen it and if so, update
last_seen_date used in later scripts.   If it hasn't seen it,
it then creates a new record.

This script also has a call to the Google Maps reverse geocoding
API that can be used to also collect the City / State for each 
sensor.  I used this for other projects.

## download_raw_purple_air_readings.py

Download the raw purple air data from thingspeak within
a geographic area of interest provided as arguments to the 
script (see below).   We then loop over each sensor in the 
sqlite database that are within that bounding box and
download daily raw data for each  sensor from it's installation 
date through today.  This data is currently downloaded as CSV
into the data/thingspeak folder

    python download_raw_purple_air_readings.py 33.994905 34.057430 -118.510778 -118.427848 72

## merge-purple-air-data.py
The previous step can create hundreds of CSV files.  This 
simple procedure merges them into one single file.


# Analyze data

Coming soon