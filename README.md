# influx-weather
A script for pulling current weather data into InfluxDB from a range of sources

## Installation
- Clone Repository
- Change working directory in `influx-weather.service` to location of cloned repo
- Install untangle using `sudo pip3 install untangle`
- Install lxml using `pip3 install lxml`
- Install the [influxdb python client](https://github.com/influxdata/influxdb-python) using `sudo pip3 install influxdb`
- Make copies of each of the config files, and remove the `.example` placeholder in the name.
- Copy the `influx-weather.service` file to `/etc/systemd/system/`

## Credits
Inspiration for this script came from [Barry Carey](https://github.com/barrycarey/Plex-Data-Collector-For-InfluxDB)
