import json, csv
import time, sys
import requests
import logging
from datetime import datetime
import config
import sqlite3


def get_google_reverse_geocode(lat, long, api_key=None, return_full_response=False):
  """
  Get geocode results from Google Maps Geocoding API.

  Note, that in the case of multiple google geocode reuslts, this function returns details of the FIRST result.

  I have restricted access to the Google Geocoding API to a set of IP addresses.

  @param address: String address as accurate as possible. For Example "18 Grafton Street, Dublin, Ireland"
  @param api_key: String API key if present from google.
                  If supplied, requests will use your allowance from the Google API. If not, you
                  will be limited to the free usage of 2500 requests per day.
  @param return_full_response: Boolean to indicate if you'd like to return the full response from google. This
                  is useful if you'd like additional location details for storage or parsing later.
  """

  long_state_name, short_country_name = '', ''
  geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?latlng={ll}&result_type=administrative_area_level_1".format(
    ll=','.join([str(lat), str(long)]))
  geocode_url = geocode_url + "&key={}".format(api_key)
  # Ping google for the results:
  results = requests.get(geocode_url, verify=False)
  # Results will be in JSON format - convert to dict using requests functionality
  results = results.json()
  status = results['status']
  print("  Status from Google {}".format(status))
  if (status == 'OK'):
    long_state_name = results['results'][0]['address_components'][0]['long_name']
    short_country_name = results['results'][0]['address_components'][1]['short_name']
  return status, long_state_name, short_country_name


def get_details_on_sensor(sensor_id):
  sensordetail_url = 'https://www.purpleair.com/json?show={}'
  with requests.get(sensordetail_url.format(str(sensor_id)), verify=False) as jsonf:

    sensor_version = 'unknown'
    sensor_uptime = 0
    RSSI = None
    created = None
    if 'results' in jsonf.json() and len(jsonf.json()['results']) > 0:
      sensor_details = jsonf.json()['results'][0]

      if 'Version' in sensor_details:
        sensor_version = sensor_details['Version']
      if 'Uptime' in sensor_details:
        sensor_uptime = sensor_details['Uptime']
      if 'RSSI' in sensor_details:
        RSSI = sensor_details['RSSI']
      if 'Created' in sensor_details:
        created = sensor_details['Created']

    return created, sensor_version, sensor_uptime, RSSI


def sensor_in_db(cur, sensor_id):
  sql = "SELECT sensor_id, processed FROM sensor_info " \
        "WHERE sensor_id={}".format(sensor_id)

  cur.execute(sql)
  record = cur.fetchone()
  result = 0
  processed = 0
  if record != None:
    result = record[0]
    processed = record[1]

  return result, processed


def update_sensor(cur, sensor_id, parent_id, label, ts_primary_id, ts_primary_read_key,
                  ts_secondary_id, ts_secondary_read_key,
                  lat, lon, lastseen, type, location, hidden, isowner,
                  age, sensor_registered_date, sensor_version, RSSI):
  sql = "UPDATE sensor_info " \
        "SET sensor_name=?, " \
        "parent_id=?, " \
        "latitude=?, " \
        "longitude=?," \
        "sensor_location=?, " \
        "hidden=?, " \
        "thingspeak_primary_id=?, " \
        "thingspeak_primary_read_key=?, " \
        "thingspeak_secondary_id=?, " \
        "thingspeak_secondary_read_key=?, " \
        "last_seen_date=?, " \
        "type=?, " \
        "created_date=?, " \
        "version=?, " \
        "RSSI=?, " \
        "processed=? " \
        "WHERE sensor_id=?"

  sql_params = (label, parent_id, lat, lon, location, hidden, ts_primary_id, ts_primary_read_key,
                ts_secondary_id, ts_secondary_read_key, lastseen, type,
                sensor_registered_date, sensor_version, RSSI, 1, sensor_id)

  cur.execute(sql, sql_params)


def insert_sensor(cur, sensor_id, parent_id, label, ts_primary_id, ts_primary_read_key,
                  ts_secondary_id, ts_secondary_read_key,
                  lat, lon, lastseen, type, location, hidden, isowner,
                  age, sensor_registered_date, sensor_version, RSSI):
  sql = "INSERT INTO sensor_info " \
        "(sensor_id, parent_id, sensor_name, " \
        "latitude, " \
        "longitude," \
        "sensor_location, " \
        "hidden, " \
        "thingspeak_primary_id, " \
        "thingspeak_primary_read_key, " \
        "thingspeak_secondary_id, " \
        "thingspeak_secondary_read_key, " \
        "last_seen_date, " \
        "type, " \
        "created_date, " \
        "version, " \
        "RSSI," \
        "processed)" \
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)"

  sql_params = (sensor_id, parent_id, label, lat, lon, location, hidden, ts_primary_id, ts_primary_read_key,
                ts_secondary_id, ts_secondary_read_key, lastseen, type,
                sensor_registered_date, sensor_version, RSSI)
  cur.execute(sql, sql_params)


def augment_purple_air_sensor_data(G_API_KEY):
  """Process the purple air sensor index.  The source file is at
  http://www.purpleair.com/json.  If we wanted this in real time, we
  would use requests and download.

  Process loops over every sensor in the purple air list,
  checks whether the sensor is in the database and whether it's
  been processed.   If it's not in the database, it will add a new record
  if it's in the database, it updates a few bits, most important the last_seen_date
  field that is used to bound the data extraction in the download_raw_purple... script.
  """

  pa_count = 0
  sensor_id = 0
  sensorlist_url = 'https://www.purpleair.com/json'

  conn = sqlite3.connect('data/purple_air.db')
  cur = conn.cursor()

  with requests.get(sensorlist_url, verify=False) as jsonf:
    palist = jsonf.json()
    pa_sensors = palist['results']

    for sensor in pa_sensors:

      time.sleep(1)
      pa_count += 1

      if pa_count % 100 == 0:
        conn.commit()
        print("COMMITED {}".format(sensor_id))

      type = ''
      parent_id = None
      sensor_id = sensor['ID']

      exists, processed = sensor_in_db(cur, sensor_id)

      # if we've already processed this sensor, carry on.
      if processed == 1:
        continue

      print("Sensor {} {} exists {}".format(sensor_id, sensor['Label'], exists))

      if 'ParentID' in sensor:
        parent_id = sensor['ParentID']
      sensor_label = sensor['Label']
      thingspeak_primary_id = sensor['THINGSPEAK_PRIMARY_ID']
      thingspeak_primary_key = sensor['THINGSPEAK_PRIMARY_ID_READ_KEY']
      thingspeak_secondary_id = sensor['THINGSPEAK_SECONDARY_ID']
      thingspeak_secondary_key = sensor['THINGSPEAK_SECONDARY_ID_READ_KEY']
      if 'Type' in sensor:
        type = sensor['Type']
      hidden = 1 if (sensor['Hidden'] == 'true') else 0
      last_seen = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(sensor['LastSeen']))

      # If we don't know where it is, it won't have much value - that said, it appears
      # that the one i found, C4 Road Julia Vargas Street has the Lat/Lon on the secondary
      # record, so could go look there if I cared enough.  Seems to be a rare case
      if 'Lat' in sensor and 'Lon' in sensor:
        sensor_lat = sensor['Lat']
        sensor_lon = sensor['Lon']
        sensor_location = 'unknown'
        if 'DEVICE_LOCATIONTYPE' in sensor:
          sensor_location = sensor['DEVICE_LOCATIONTYPE']

        sensor_registered, sensor_version, sensor_uptime, RSSI = get_details_on_sensor(sensor_id)
        sensor_registered_date = datetime.utcfromtimestamp(sensor_registered) if (sensor_registered != None) else None

        state_name, country_code = '', ''

        # status, state_name, country_code = get_google_reverse_geocode(lat, lon,
        #                                                         G_API_KEY, False)

        if exists:
          update_sensor(cur, sensor_id, parent_id, sensor_label, thingspeak_primary_id, thingspeak_primary_key,
                        thingspeak_secondary_id, thingspeak_secondary_key,
                        sensor_lat, sensor_lon, last_seen, type, sensor_location, hidden, 0,
                        1, sensor_registered_date, sensor_version, RSSI)
        else:
          print("Inserting {} named {}".format(sensor_id, sensor_label))
          insert_sensor(cur, sensor_id, parent_id, sensor_label, thingspeak_primary_id, thingspeak_primary_key,
                        thingspeak_secondary_id, thingspeak_secondary_key,
                        sensor_lat, sensor_lon, last_seen, type, sensor_location, hidden, 0,
                        1, sensor_registered_date, sensor_version, RSSI)
      else:
        print("No Lat Lon for {} with hidden {}".format(sensor_label, hidden))

  conn.commit()
  conn.close()


def main(argv):
  """Initiate the process
  The API_Key is used to do a reverse geocode to identify the City/State if desired.  For now, I
  am not doing this, so can be ignored.  The value would be in the config.py file simply as
  google_api_key = 'A3FE5...'
  """
  API_Key = config.google_api_key

  logger = logging.getLogger("root")
  logger.setLevel(logging.DEBUG)
  ch = logging.StreamHandler()
  ch.setLevel(logging.DEBUG)
  logger.addHandler(ch)

  augment_purple_air_sensor_data(API_Key)


if __name__ == "__main__":
  main(sys.argv[1:])
