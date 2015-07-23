pystats
===
pystats is a simple and useful program.It is a little tool to collect and format data from log(e.g., nginx log).Data wiil be sent to [StatsD](https://github.com/etsy/statsd) backend service.StatsD usually works with [graphite](http://graphite.readthedocs.org/en/latest/index.html) together.pystats is a small piece of puzzle to build a monitor system.

install
---
* server: 
  install graphite; install statsd
* client: 
  install [pystatsd](https://github.com/jsocol/pystatsd)
* clone the project
* start the pystats:
  python pystats.py -f access.log

usage
---
