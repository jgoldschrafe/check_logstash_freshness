About
=====
This check takes an optional ElasticSearch query and determines whether
Logstash has ingested any matching data within the specified freshness
interval. It's intended to catch cases where the Logstash/ES processes
are running like everything is totally fine, but nothing is actually
fine at all.

Requirements
============
This plugin requires

* Python 2.7+ (or 2.6 with nagiosplugin patched to work)
* nagiosplugin
* pyelasticsearch

Invocation
==========

    # Check that Logstash has received logs from somehost within past 10 minutes
    ./check_logstash_freshness -c 0:900 -w 0:600 -p logstash \
        -u http://logstash:9200 -q '@source:somehost'

