# Copyright (c) 2008 Drew Smathers
# See LICENSE for details. 

"""
Memcache Multi-Client Proxy and Node Locator components.
"""

from twisted.internet import defer
from twisted.internet.protocol import ReconnectingClientFactory, ClientFactory
from twisted.python import failure
from twisted.protocols.memcache import MemCacheProtocol

from zope.interface import Interface, Attribute, implements

import txmemcache.hash
from twisted.internet.error import ConnectionRefusedError
from twisted.trial.unittest import TestCase
from twisted.internet.defer import gatherResults, DeferredList
from twisted.python import log

import urllib
import collections
from functools import partial

class INodeLocator(Interface):

    addresses = Attribute("""
        A sequence of (hostname, port) C{tuple}s.
        """)

    hashFunction = Attribute("""
        A hashing function
        """)

    def locate(key):
        """Locate a node based on the given key
        """

    def add(address):
        """Add (hostname,port) C{tuple} to the pool
        """

    def remove(address):
        """Remove (hostname,port) C{tuple} from the pool.
        """

    def reconfigure(addresses, hashFunction=None):
        """Reconfigure locator with new addresses.
        """


class NoMoreNodes(Exception):
    """
    Raised if pool is empty due to connection failures, etc.
    """

class NodeLocator(object):

    implements(INodeLocator)

    hashFunction = None

    def __init__(self, addresses, hashFunction=None):
        self.reconfigure(addresses, hashFunction)

    def locate(self, key):
        if not len(self.addresses):
            raise NoMoreNodes, 'No addresses in pool'
        h = self.hashFunction(key)
        return self.addresses[h % len(self.addresses)]

    def add(self, address):
        k = tuple(address)
        assert self._bucketIndices.has_key(k),(
           'Cannot add an address not supplied in reconfigure(): %s' %
           address)
        for idx in self._bucketIndices[k]:
            self._addrBuckets[idx] = k
        self.addresses = [ addr for addr in self._addrBuckets
                if addr is not None ]

    def remove(self, address):
        k = tuple(address)
        if not self._bucketIndices.has_key(k):
            return
        for idx in self._bucketIndices[k]:
            self._addrBuckets[idx] = None
        self.addresses = [ addr for addr in self._addrBuckets
                if addr is not None ]


    def reconfigure(self, addresses, hashFunction=None):
        self.addresses = addresses
        self._addrBuckets = list(addresses)
        # create mapping of (address,port) buckets
        self._bucketIndices = {}
        for (idx, addr) in enumerate(addresses):
            b = self._bucketIndices.setdefault(tuple(addr), [])
            b.append(idx)
        self.hashFunction = (hashFunction or self.hashFunction or
                txmemcache.hash.CRC32)

class ClientPool(object):
    """
    Manage a pool of memcached connections.
    
    This pool precreates a certain number of memcached clients.
    If it exhausts the available connections it can optionally create more.
    If it doesn't create more it will simply return a cache miss.
    """
    def __init__(self,servers,count,grow_pool=True):
        self.count = count
        self.grow_pool = grow_pool
        self.servers = servers
        self.clients = None
        self.used = None
        self.avail_methods = dict(avail_methods)

    def err_back(self,*args,**kwargs):
        print args,kwargs
        
    def connectTCP(self):
        c = [MultiClientProxy(self.servers,urlencode=True,debug=True) for i in range(self.count)]
        dl = [client.connectTCP() for client in c]
        dl = DeferredList(dl)
        dl.addCallback(self._connected, c)
        return dl.addErrback(self.err_back)
    
    def _connected(self, results, clients):
        self.clients = collections.deque(clients)
        self.used = collections.deque()
        
    def _do(self, name, *args, **kwargs):
        if self.clients:
            c = self.clients.popleft()
            return self._call_final(None,c,name, args, kwargs).addErrback(log.err)
        elif self.grow_pool:
            c = MultiClientProxy(self.servers, urlencode=True)
            d = c.connectTCP()
            d.addCallback(self._call_final,c,name, args, kwargs)
            d.addErrback(self.err_back)
            return d
        else:
            return None
        
    def _call_final(self, r, c, name, args, kwargs):
        self.used.append(c)
        f = getattr(c,name, None)
        if f: 
            print f,args,kwargs
            r = f(*args,**kwargs)
            return r
        else:
            raise AttributeError("No method/attribute [%s]" % name)
    
    def _delayed_do(self, res, proxy):
        return res
        
    def __getattr__(self, name):
        if name in self.avail_methods.keys():
            return partial(self._do,name)
        else:
            raise AttributeError("No method/attribute [%s]" % name)

    def _cb(self,response, c, _cb, *args, **kwargs):
        """
        Final callback from memcached that dispatches result to client.
        
        There is a chance this function won't be called on error.
        This will cause a leak in self.used.
        
        There probably should be something that periodically cleans up
        the self.used set of connections to remove dead ones.
        """
        self.used.remove(c)
        self.clients.appendleft(c)
        _cb(response, *args,**kwargs)


class _MemCacheProtocol(MemCacheProtocol):

    def connectionMade(self):
        if self.factory.clientProxy.debug:
            log.msg('connected to %r,%r' % self.factory.address)
        MemCacheProtocol.connectionMade(self)
        self.factory.clientProxy.nodeLocator.add(
                self.factory.address)
        self.factory.clientProxy._protocols[
                tuple(self.factory.address) ] = self


class MemCacheClientFactory(ClientFactory):
    protocol = _MemCacheProtocol
    maxDelay = 1800
    factor = 1.6180339887498948
    maxRetries = 2

    def __init__(self, reactor, address, clientProxy, deferred):
        self.reactor = reactor
        self.address = address
        self.clientProxy = clientProxy
        self.deferred = deferred

    def clientConnectionFailed(self, connector, reason):
        self.clientProxy.nodeLocator.remove(self.address)
        if hasattr(self, 'deferred') and not self.deferred.called:
            self.reactor.callLater(0, self.deferred.errback, reason)
            del self.deferred
        ClientFactory.clientConnectionFailed(
                self, connector, reason)

    def clientConnectionLost(self, connector, unused_reason):
        self.clientProxy.nodeLocator.remove(self.address)
        if hasattr(self, 'deferred') and not self.deferred.called:
            self.reactor.callLater(0, self.deferred.errback, unused_reason)
            del self.deferred
        ClientFactory.clientConnectionLost(
                self, connector, unused_reason)

    def buildProtocol(self, addr):
        p = ClientFactory.buildProtocol(self, addr)
        if not hasattr(self,'deferred'):
            self.deferred = defer.Deferred()
        self.reactor.callLater(0, self.deferred.callback, p)
        del self.deferred
        return p


def _mkproxy(op, haskey):
    def _proxy(self, *p, **kw):
        try:
            assert self._ready

            args = p
            if haskey:
                first = p[0]
                if self._urlencode:
                    first = urllib.urlencode([('',first)])[1:]
                    args = tuple( [first] + list( p[1:] ) )
                node = self.nodeLocator.locate(first)
            else:
                node = self.nodeLocator.locate('')
            if self.debug:
                log.msg('using node: %r' % (node,))

            if not self._protocols.has_key(node):
                # TODO try again - node is not connected somehow
                pass
            proto = self._protocols[node]
            # Capture error and try again len(address) times
            return getattr(proto, op)(*args, **kw)
        except:
            reason = failure.Failure()
            return defer.fail(reason)
    return _proxy

avail_methods =(
            ('increment',   True),
            ('decrement',   True),
            ('replace',     True),
            ('add',         True),
            ('set',         True),
            ('checkAndSet', True),
            ('append',      True),
            ('prepend',     True),
            ('get',         True),
            ('stats',       False),
            ('version',     False),
            ('delete',      True),
            ('flushAll',    False))
class _MultiClientProxyMeta(type):

    def __new__(cls, name, bases, dct):
        for (op, haskey) in avail_methods:
            proxym = _mkproxy(op, haskey)
            proxied = getattr(MemCacheProtocol, op)
            proxym.__name__ = proxied.__name__
            proxym.__doc__ = proxied.__doc__
            dct[op] = proxym
        return type.__new__(cls, name, bases, dct)
 

class MultiClientProxy(object):

    __metaclass__ = _MultiClientProxyMeta

    def __init__(self, addresses, nodeLocator=None,
            permitConnectionFailures=True, urlencode=False, debug=False):
        self._urlencode = urlencode
        self.permitConnectionFailures = permitConnectionFailures
        self.nodeLocator = nodeLocator
        if self.nodeLocator is None:
            self.nodeLocator = NodeLocator(addresses)
        self.addresses = addresses
        self.debug = debug
        self._ready = False
        self._protocols = {}
        
    def connectTCP(self, **kw):
        from twisted.internet import reactor
        dlist = []
        for addr in self.addresses:
            if self.debug:
                log.msg('[TCP] connecting to %r' % (addr,))
            d = defer.Deferred()
            dlist.append(d)
            f = MemCacheClientFactory(reactor, addr, self, d)
            reactor.connectTCP(addr[0], addr[1], f, **kw)
        return defer.DeferredList(dlist,consumeErrors=1).addCallback(
                self._cbAllConnected)

    def connectSSL(self, contextFactory, **kw):
        from twisted.internet import reactor
        dlist = []
        for addr in self.addresses:
            if self.debug:
                log.msg('[SSL] connecting to %r' % (addr,))
            d = defer.Deferred()
            dlist.append(d)
            f = MemCacheClientFactory(reactor, addr, self, d)
            reactor.connectSSL(addr[0], addr[1], f, contextFactory **kw)
        return defer.DeferredList(dlist,consumeErrors=1).addCallback(
                self._cbAllConnected)

    def _cbAllConnected(self, result):
        for (ok,r) in result:
            if not ok and not self.permitConnectionFailures:
                r.trap()
        self._ready = True
    
    def _ebAllConnected(self, reason):
        if self.permitConnectionFailures:
            self._ready = True
            return
        reason.trap()
        
    def loseConnection(self):
        for p in self._protocols.values():
            p.transport.loseConnection()

class TestMemcache(TestCase):
    addresses = [("127.0.0.1",11211)]
    
    def test_proxy(self):
        def _get_done(resp):
            self.assertEqual(resp[1],'bar')
            m.loseConnection()

        def _set_done(*args,**kwargs):
            d = m.get("foo").addCallback(_get_done)
            return d
            
        def _cb(ignored):
            self.assertTrue(self.addresses[0] in m._protocols)
            d = m.set("foo","bar").addCallback(_set_done)
            return d
            
        m = MultiClientProxy(self.addresses, debug=True)
        d = m.connectTCP().addCallback(_cb)
        return d
    
    def test_proxy_fail(self):
        self.addresses = [("127.0.0.1",11212)]
        
        def _eb(err):
            err.trap(ConnectionRefusedError)
            
        m = MultiClientProxy(self.addresses, debug=False,permitConnectionFailures=False)
        d = m.connectTCP().addErrback(_eb)
        def foo(result):
            self.assertEqual(result,None)
        d.addCallback(foo)
        return d