import requests
import csv
import os, sys
import spatial_utils
import sqlite3
import logging
from datetime import datetime, timedelta

def get_raw_purple_air_reading(channel, thingspeak_key, start_date, end_date, sensor_instance):
  """
  Get Raw Purple Air particulate matter data from Thinkspeak website in CSV format.

  Thinkspeak offers an API to download their data in one-day chunks as CSV.

  @param channel: String representing the thingspeak id - each purple air
  monitor has 2 sensors and each sensor has 2 channels
  @param api_key: String API key if present from thingspeak
  @param start_date: The day of the request
  @param end_date: The day after the request
  """

  thinkspeak_url = "https://thingspeak.com/channels/{channel}/feed.csv?api_key={api}&" \
                "offset=0&average=&round=2&start={start}&end={end}".format \
    (channel=channel,api=thingspeak_key,start=start_date, end=end_date)

  print(thinkspeak_url)
  file_name = os.getcwd() + '/data/thingspeak/thingspeak-{channel}-{instance}-{start}.csv'.format(channel=channel,
                                                                          instance=sensor_instance,
                                                                          start=start_date[:10])
  if not os.path.exists(file_name):
    with requests.Session() as s:
      download = s.get(thinkspeak_url,
                     verify=False)
      decoded_content = download.content.decode('utf-8')

      with open(file_name, 'w') as channel_date_part_file:
        channel_date_part_file.writelines(decoded_content)

      daily_file = os.stat(file_name)

      if daily_file.st_size < 100:
        os.remove(file_name)

def daterange(start_date, end_date):
  for n in range(int((end_date - start_date).days)):
    yield start_date + timedelta(n)

def download_purple_air_raw_data(sensor_id, created_date, last_seen_date,
                                 thingspeak_id, thingspeak_key, sensor_instance, process_from_start):

  if process_from_start == 0:
    created_date = datetime.strptime(created_date, '%Y-%m-%d %H:%M:%S')
  else:
    created_date = datetime.now() - timedelta(days=process_from_start)

  last_seen_date = datetime.strptime(last_seen_date, '%Y-%m-%d %H:%M:%S')
  # Use the following to hard code the date ranges as needed
  # created_date = datetime.strptime('2019-08-22 00:00:00', '%Y-%m-%d %H:%M:%S')
  # last_seen_date = datetime.strptime('2019-08-23  23:59:59', '%Y-%m-%d %H:%M:%S')
  # last_seen_date = datetime.strptime('2020-01-20 23:59:59', '%Y-%m-%d %H:%M:%S')

  print ("Downloading data for {} starting {} and ending {}".format(sensor_id, created_date, last_seen_date))
  for single_date in daterange(created_date, last_seen_date):

    start_date = single_date.strftime("%Y-%m-%d 00:00:00")
    end_date = single_date.strftime("%Y-%m-%d 23:59:59")

    get_raw_purple_air_reading(thingspeak_id, thingspeak_key, start_date, end_date, sensor_instance)

def bounding_box_exceeds_limits(min_lat, max_lat, min_long, max_long):
  """To limit the number of sensors we might download given a certain study area,
  we restrict the decimal degree bounding box to < 2 DD.  This can be overriden in
  the script's call"""
  if abs(min_lat - max_lat) > 2 or abs(min_long - max_long) > 2:
    return True
  else:
    return False

# python download_raw_purple_air_readings.py 33.994905 34.057430 -118.510778 -118.427848 72
def main(argv):
  """Initiate the script collecting the bounding box coordinates and possibly
     a maximum bounding box override


     This script takes 6 possible arguments
     min_lat - the minimum latitude
     max_late - the maximum latitude
     min_long - the minimum longitude
     max_long - the maximum longitude
     process - whether to process from the beginning; 0 = yes, NN > 0 is days back from today
     override - whether to overide the maximum bounding box option of 2 decimal degrees
     """

  if len(argv) < 5:
    print("Script requires a bounding box of floating numericals in this order: min_lat, max_lat, "
          "min_long and max_long as well as a flag indicating whether to download for alltime (0)"
          "or the past year (1) and whether to override the bounding box size limits (2 decimal degrees)")
  else:
    min_lat = float(argv[0])
    max_lat = float(argv[1])
    min_long = float(argv[2])
    max_long = float(argv[3])
    process = int(argv[4])
    proceed = True
    if bounding_box_exceeds_limits(min_lat, max_lat, min_long, max_long):
      if len(argv) == 6:
        override = argv[5]
        if not override:
          print("The bounding box must be less than 2 decimal degrees.")
          proceed = False

    if proceed:
      logger = logging.getLogger("root")
      logger.setLevel(logging.DEBUG)
      # create console handler
      ch = logging.StreamHandler()
      ch.setLevel(logging.DEBUG)
      logger.addHandler(ch)

      processed_sensors = []
      download_tracker_file_name = './data/download-tracker.txt'
      if os.path.exists(download_tracker_file_name):
        with open(download_tracker_file_name, 'r') as download_tracker_file:
          for sensor in download_tracker_file.readlines():
            processed_sensors.append(str(sensor[:-1]))

      conn = sqlite3.connect('data/purple_air.db')
      cur = conn.cursor()
      sql = "SELECT sensor_id, latitude, longitude, created_date, last_seen_date, thingspeak_primary_id," \
            "thingspeak_primary_read_key, thingspeak_secondary_id, thingspeak_secondary_read_key " \
            "FROM sensor_info " \
            "WHERE processed = 1 " \
            "AND latitude >= {min_lat} and latitude <= {max_lat} " \
            "AND longitude >= {min_lon} and longitude <= {max_lon}".format(min_lat=min_lat,
                                                                    max_lat=max_lat,
                                                                    min_lon=min_long,
                                                                    max_lon=max_long)

      with open(download_tracker_file_name, 'a') as download_tracker_file:
        for row in cur.execute(sql):
          sensor_id = row[0]
          if str(sensor_id) not in processed_sensors:
            lat = row[1]
            lon = row[2]
            # this next line predates when i put the spatial criteria in the SQL query above...
            # now, it's just a double check...
            if spatial_utils.in_area_of_interest(lat, lon, min_lat, max_lat, min_long, max_long):

              download_purple_air_raw_data(sensor_id, row[3], row[4], row[5], row[6], 'primary', process)
              download_purple_air_raw_data(sensor_id, row[3], row[4], row[7], row[8], 'secondary', process)
              download_tracker_file.write(str(sensor_id) + '\n')
          else:
            print ("We've already processed {}".format(str(sensor_id)))


      conn.close()

if __name__ == "__main__":
  main(sys.argv[1:])