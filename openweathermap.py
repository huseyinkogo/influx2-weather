import configparser, json, time, datetime, requests, sys
#import urllib2
from urllib.request import urlopen
from datetime import datetime
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
from requests.exceptions import ConnectionError

config = configparser.ConfigParser()
config.read('owm-config.ini')

cityid = config['OPENWEATHERMAP']['Location']
weatherapikey = config['OPENWEATHERMAP']['APIKey']

from influxdb_client.client.write.point import Point


def getWeatherData(cityid):
    try:
        requestURL = 'http://api.openweathermap.org/data/2.5/weather?id=' + str(cityid)+ '&units=metric' + '&APPID=' + weatherapikey

        response = requests.get(requestURL)

        data = json.loads(response.text)

        point_data = formatData(data)

        return point_data
    except ValueError as e:
        print("Unable to retrieve Open Weathermap Weather info: {}".format(e))
        return None
    except KeyError as e:
        print("Unable to retrieve Weather Underground Weather info: {}".format(e))
        return None
    except TypeError as e:
        print("Unable to retrieve Open Weathermap Weather info: {}".format(e))
        return None
    except AttributeError as e:
        print("Unable to retrieve Open Weathermap Weather info: {}".format(e))
        return None
    except:
        e = sys.exc_info()[0]
        print("Unable to retrieve Open Weathermap Weather info: {}".format(e))
        return None

def formatData(data):

    p = Point('openweathermap').\
        tag("city", data['name']).\
        tag("location_id", data['id'])
    
    if data['coord']['lon']: p = p.field('coord_lon', float(data['coord']['lon']))
    if data['coord']['lat']: p = p.field('coord_lat', float(data['coord']['lat']))
    if data['weather'][0]['id']: p = p.field('weather_id', int(data['weather'][0]['id']))
    if data['weather'][0]['main']: p = p.field('weather_main', str(data['weather'][0]['main']))
    if data['weather'][0]['description']: p = p.field('weather_description', str(data['weather'][0]['description']))
    if data['main']['temp']: p = p.field('main_temp', float(data['main']['temp']))
    if data['main']['feels_like']: p = p.field('main_feels_like', float(data['main']['feels_like']))
    if data['main']['pressure']: p = p.field('main_pressure', float(data['main']['pressure']/10))
    if data['main']['humidity']: p = p.field('main_humidity', float(data['main']['humidity']))
    if data['main']['temp_min']: p = p.field('main_temp_min', float(data['main']['temp_min']))
    if data['main']['temp_max']: p = p.field('main_temp_max', float(data['main']['temp_max']))
    if data['visibility']: p = p.field('visibility', float(data['visibility']))
    if data['wind']['speed']: p = p.field('wind_speed', float(data['wind']['speed']))
    if data['wind']['deg']: p = p.field('wind_deg', float(data['wind']['deg']))
    if 'all' in data['clouds']: p = p.field('clouds_all', float(data['clouds']['all']))
    if data['dt']: p = p.field('dt', int(data['dt']))
    if data['sys']['country']: p = p.field('sys_country', str(data['sys']['country']))
    if data['sys']['sunrise']: p = p.field('sys_sunrise', int(data['sys']['sunrise']))
    if data['sys']['sunset']: p = p.field('sys_sunset', int(data['sys']['sunset']))

    return p

def main():
    weatherdata = getWeatherData(cityid)
    
    return weatherdata

if __name__ == '__main__':
    main()
