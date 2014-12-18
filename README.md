elasticsearch-eventlet
======================

An eventlet-aware elasticsearch client.

Right now it only supports indexing (in bulk mode) and counting, which are the two things
I care about on the eventlet side.  Pull requests are welcome to add the other features.

Usage:

```python
import eventlet
from elasticsearch_eventlet import ElasticSearch

es = ElasticSearch('http://127.0.0.1:9200/')

def worker():
    docs = [{'number': i} for i in xrange(0, 1024)]
    es.bulk_index('numbers', 'number', docs)

eventlet.spawn(worker)
eventlet.join_all()
```

Of course this is a silly example, but unlike the default ElasticSearch client, it won't go
mental spawning tons of connections when there is lag which is the real point.

