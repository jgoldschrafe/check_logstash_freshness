#!/usr/bin/env python

import argparse
import datetime
from pprint import pprint
import sys

import nagiosplugin
from pyelasticsearch import ElasticSearch, ElasticHttpNotFoundError

def total_seconds(td):
    """Return the total number of seconds contained in a TimeDelta.

    Not strictly speaking accurate, since we don't account for leap
    seconds, but this does get us usefully close.
    """
    return td.days * 24 * 60 * 60 + td.seconds

class LogstashFreshness(nagiosplugin.Resource):
    """Logstash data freshness checker.

    Query Logstash for the most recent single log entry, then check the
    timestamp on it for freshness.
    """
    def __init__(self, opts):
        self.opts = opts

    def probe(self):
        result = self._get()
        if result:
            freshness = self._freshness(result)
            yield nagiosplugin.Metric('freshness', freshness, min=0,
                                      context='freshness')

    def _freshness(self, result):
        """Determine freshness of an ES query result
        """
        fmt = '%Y-%m-%dT%H:%M:%S.%f'
        hits = result['hits']['hits']
        if len(hits) > 0:
            timestr = hits[0]['_source']['@timestamp']
        else:
            raise Exception("No hits returned from ES")

        # Python 2.6 does not support timedelta.total_seconds() or the
        # %z specifier to strptime(), and Python in general doesn't
        # support milliseconds in strptime, so we do horrible things to
        # the timestamp string to make it parseable.
        #
        #   2013-01-23T09:08:29.514Z-0500 -> 2013-01-23T14:08:29.514000
        #
        # This only works if our time is UTC. There's better ways to do
        # this in 2.6, I'm sure.
        timestr = timestr[0:23] + '000'

        timestamp = datetime.datetime.strptime(timestr, fmt)
        delta = (datetime.datetime.utcnow() - timestamp)
        return total_seconds(delta)

    def _get(self):
        """Build and run the ES query
        """
        opts = self.opts

        es = ElasticSearch(opts.url)
        query = {'sort': {'@timestamp': 'desc'},
                 'size': 1}

        if opts.query:
            query['query'] = {
                'filtered': {
                    'query': {
                        'query_string': {
                            'query': opts.query
                        }
                    }
                }
            }

        # ElasticSearch allows us to pass an array of indices. However,
        # it will throw an exception if any of these don't exist. This
        # isn't the right behavior, because there may not actually be
        # a logstash index from X days ago. Instead, we need to iterate
        # through the daily log indexes in reverse order until we get a
        # non-error response.
        result = None
        for index in self._indexes():
            try:
                result = es.search(query, index=index)
                break
            except ElasticHttpNotFoundError, e:
                pass

        return result

    def _indexes(self):
        """Determine the indexes to be searched based on the time period

        Given the user's provided critical freshness threshold, return
        all indexes that may contain hits for the log data being
        searched.
        """
        indexes = []
        now = datetime.datetime.utcnow()
        range = nagiosplugin.Range(self.opts.critical)
        delta = datetime.timedelta(seconds=range.end)
        timefmt = '{prefix}-%Y.%m.%d'.format(prefix=self.opts.prefix)

        if delta.seconds > 0:
            delta = datetime.timedelta(days=(delta.days + 1))

        while (delta.days >= 0):
            index = (now - delta).strftime(timefmt)
            indexes.append(index)
            delta = datetime.timedelta(days=(delta.days - 1))

        return reversed(indexes)


@nagiosplugin.guarded
def main():
    argp = argparse.ArgumentParser(description=__doc__)
    argp.add_argument('-w', '--warning', metavar='RANGE',
            default='0:600',
            help='return warning if freshness is outside RANGE')
    argp.add_argument('-c', '--critical', metavar='RANGE',
            default='0:900',
            help='return critical if freshness is outside RANGE')
    argp.add_argument('-p', '--prefix', default='logstash',
            help='ElasticSearch index prefix')
    argp.add_argument('-q', '--query',
            help='ElasticSearch query string')
    argp.add_argument('-t', '--timeout', default=10, type=int,
            help='Timeout in seconds')
    argp.add_argument('-u', '--url', default='',
            help='Graphite render URL to query')
    opts = argp.parse_args()

    check = nagiosplugin.Check(
            LogstashFreshness(opts=opts),
            nagiosplugin.ScalarContext('freshness', opts.warning,
                                       opts.critical))
    check.main(timeout=opts.timeout)

if __name__ == '__main__':
    main()

