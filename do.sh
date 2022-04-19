#!/bin/sh
curl -X DELETE -u 'admin:admin' 'http://localhost:8980/opennms/rest/graphml/netflow'
sleep 10
curl -X POST -H "Content-Type: application/xml" -u 'admin:admin' -d@/tmp/graph.xml 'http://localhost:8980/opennms/rest/graphml/netflow'
