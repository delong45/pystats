pystats
===
pystats is a simple and useful program.It is a little tool to collect and format data from log(e.g., nginx log).Data wiil be sent to [StatsD](https://github.com/etsy/statsd) backend service.StatsD usually works with [graphite](http://graphite.readthedocs.org/en/latest/index.html) together.pystats is a small piece of puzzle to build a monitor system.

install
---
* server: 
  
    install graphite
    
    install statsd
* client: 
  
    install [pystatsd](https://github.com/jsocol/pystatsd)
* clone the project
* start the pystats:
  
    python pystats.py -f access.log

usage
---
    Usage: pystats.py [options]

    Options:
      -h, --help            show this help message and exit
      -f FILE, --file=FILE  file to tail into statsD
      -H HOST, --host=HOST  destination StatsD host server
      -p PORT, --port=PORT  destination StatsD port
      -b BEGIN, --begin=BEGIN
                        where does tail begin, 0 means beginning, 1 means
                        current, 2 means end
      -c CATEGORY, --category=CATEGORY
                        which category of file to collect
                        
reference
---
[Getting Started with Monitoring using Graphite](http://www.infoq.com/articles/graphite-intro)
[10 Things I Learned Deploying Graphite](https://kevinmccarthy.org/2013/07/18/10-things-i-learned-deploying-graphite/)
