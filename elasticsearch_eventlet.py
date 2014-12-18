# Copyright (c) 2014, William Pitcock <nenolod@dereferenced.org>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import simplejson as json
import eventlet
import erequests
import logging

class ElasticSearch(object):
    def __init__(self, base_url='http://127.0.0.1:9200/', size=10, lazy_indexing_threshold=1000, logger=None):
        """Initialize an ElasticSearch client.
           Optional Params:
               size: connection pool size (default 10)
               base_url: base url for the elasticsearch worker node
               lazy_indexing_threshold: number of index entries to lazily commit.
                                        set to None to make commits happen in realtime."""
        self.pool = eventlet.GreenPool(size)
        self.base_url = base_url
        self.session = erequests.Session()
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger(__name__)

        self.lazy_indexing_threshold = lazy_indexing_threshold
        if self.lazy_indexing_threshold:
            self.lazy_queues = dict()

        if self.base_url[-1] != '/':
            self.base_url += '/'

    def map(self, requests):
        def submit(r):
            try:
                return r.send()
            except Exception as e:
                return e
        jobs = [self.pool.spawn(submit, r) for r in requests]
        for j in jobs:
            yield j.wait()

    def map_one(self, request):
        return list(self.map([request]))[0]

    def _flushqueue(self, index):
        if not self.lazy_indexing_threshold:
            return
        if not index in self.lazy_queues:
            return
        if len(self.lazy_queues[index]) > self.lazy_indexing_threshold:
            docs = self.lazy_queues[index]
            self.lazy_queues[index] = list()
            self.bulk_index(index, docs)

    def build_url(self, index=None, doc_type=None, action=None):
        uri = self.base_url
        if index:
            uri += index + '/'
        if doc_type:
            uri += doc_type + '/'
        if action:
            uri += action
        return uri

    def count(self, index, doc_type=None, body=None):
        method = 'POST' if body else 'GET'
        url = self.build_url(index, doc_type, '_count')

        self._flushqueue(index)

        asr = erequests.AsyncRequest(method, url, self.session)
        if body:
            asr.prepare(data=json.dumps(body))
        res = self.map_one(asr)
        try:
            return res.json()
        except:
            self.logger.info('exception: ' + repr(res) + ' payload: ' + repr(res.text) + ' url: ' + url)

    def bulk_index(self, index, docs, id_field='_id', parent_field='_parent'):
        chunks = []
        for doc in docs:
            if not '_type' in doc:
                raise ValueError('document is missing _type field.')

            action = {'index': {'_index': index, '_type': doc.pop('_type')}}

            if doc.get(id_field) is not None:
                action['index']['_id'] = doc[id_field]

            if doc.get(parent_field) is not None:
                action['index']['_parent'] = doc.pop(parent_field)

            chunks.append(json.dumps(action))
            chunks.append(json.dumps(doc))

        payload = '\n'.join(chunks) + '\n'
        url = self.build_url(index, None, '_count')
        asr = erequests.AsyncRequest('POST', url, self.session)
        asr.prepare(data=payload)

        r = self.map_one(asr)
        try:
            return r.json()
        except:
            pass

    def index(self, index, doc_type, doc):
        doc['_type'] = doc_type

        if self.lazy_indexing_threshold:
            if index not in self.lazy_queues:
                self.lazy_queues[index] = [doc]
            else:
                self.lazy_queues[index].append(doc)
            self._flushqueue(index)
            return

        return self.bulk_index(index, [doc])
