import configparser
import requests
import importlib

from datetime import datetime
import json
import time
import datetime

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
from influxdb_client.client.write.point import Point

config = configparser.ConfigParser()
config.read('config.ini')

delay = float(config['GENERAL']['Delay'])
output = bool(config['GENERAL'].get('Output', fallback=True))
# print(output)

influxURL = config['INFLUXDB']['URL']
influxTOKEN = config['INFLUXDB']['token']
influxORG = config['INFLUXDB']['org']
influxBucket = config['INFLUXDB']['bucket']

Sources = json.loads(config['WEATHER'].get('Sources'))

influx_client = InfluxDBClient(url = influxURL, token = influxTOKEN,
        org = influxORG)
write_api = client.write_api(write_options=ASYNCHRONOUS)

#return a list of payloads to send to influxdb
def getSourceData(source):
    
    lib = importlib.import_module(source)

    sourceData = lib.main()

    return sourceData

def sendInfluxData(data):

    if output:
        print(type(data))

    try:
        write_api.write(bucket = influxBucket, org = influxORG, record = data)
    except Exception as e:
        print('ERROR: Failed To Write To InfluxDB')
        print(e)

    if output:
        print('Written To Influx: {}'.format(data))


def main():
    while True:
        for source in Sources:

            sourceData = getSourceData(source)
            
            #only send the data if there is non-null data to send
            if sourceData is not None:
                sendInfluxData(sourceData)

        time.sleep(delay)

if __name__ == '__main__':
    main()
