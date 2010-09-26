#!/usr/bin/env python

# Copyright (c) 2008 Drew Smathers
# See LICENSE for details. 

import sys

from twisted.internet import reactor, defer

from txmemcache import client, hash

# USAGE
#
# python -m examples/simple.py hostx:portx[,hosty:porty,...] <command> [args]
#
# Example:
#
# python -m examples/simple.py localhost:11211,localhost:21212 set k hello

servers = []
for host in sys.argv[1].split(','):
    if ':' in host:
        server, port = host.split(':')
        port = int(port)
    else:
        server = host
        port = 11211
    servers.append((server, port))
#servers = [ (a.split(':')[0],int(a.split(':')[1]))
#            for a in sys.argv[1].split(',') ]
command = sys.argv[2]
args = sys.argv[3:]

@defer.inlineCallbacks
def run():
    proxy = client.MultiClientProxy( servers, urlencode=True,
            nodeLocator=client.NodeLocator( servers,
                hashFunction=hash.JAVA_NATIVE ) )
    yield proxy.connectTCP(timeout = 2)
    result = yield getattr(proxy, command)(*args)
    print 'Result:', type(result) is tuple and result[1] or result
    reactor.stop()


reactor.callLater(0.5, run)
reactor.run()

