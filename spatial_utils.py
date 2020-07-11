def in_woolsey_fire_area(lat, long):
  """"Based on coordinates from this map,
  http://cdfdata.fire.ca.gov/pub/cdf/images/incidentfile2282_4275.pdf,
  determine whether the point is in the 2018 Woolsey fire burn
  area.   This is a simple bounding box approach, to be more
  accurate you would use geospatial analysis package.  Could
  use the shapefile found at this website
  https://egis3.lacounty.gov/dataportal/2018/11/21/woolsey-fire-nov-2018-gis-data-applications/"""
  min_lat = 33.856
  max_lat = 34.211
  min_long = -118.7114
  max_long = -118.2511

  return in_area_of_interest(lat, long, min_lat, max_lat, min_long, max_long)


def in_area_of_interest (lat, long, min_lat, max_lat, min_long, max_long):
  """Determine whether a point is within a bounding box"""

  if lat is None or long is None:
    return False

  lat = float(lat)
  long = float(long)

  if ((lat >= min_lat and lat <= max_lat) and
    (long >= min_long and long <= max_long)):
    return True

  return False