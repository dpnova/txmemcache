# Copyright (c) 2008 Drew Smathers
# See LICENSE for details. 

_zlib = True
try:
    import zlib
except ImportError:
    _zlib = False

_hashlib = True
try:
    import hashlib
except ImportError:
    _hashlib = False
    import md5 as hashlib

from binascii import crc32


"Compliant crc32 hashing function. (Most commonly used by clients.)"
CRC32 = crc32

if _zlib:
    "zlib-based crc32 hashing function"
    ZLIB_CRC32 = zlib.crc32
else:
    def ZLIB_CRC32(key):
        raise RuntimeError("zlib not available")

def _signed_int32(n):
    n = n & 0xffffffff
    return ((1 << 31) & n) and (~n + 1) or n

def JAVA_NATIVE(key):
    """
    Used by JVM for java.lang.String and hence by some
    popular java memcached clients as default - such as
    http://www.whalin.com/memcached.

    s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1]
    """
    h = 0
    l = len(key)
    for (idx,c) in enumerate(key):
        h += ord(c)*31**(l-(idx+1))
    return _signed_int32(h)

def KETAMA(key):
    """
    MD5-based hashing algorithm used in consistent hashing scheme
    to compensate for servers added/removed from memcached pool.
    """
    d = hashlib.md5(key).digest()
    c = _signed_int32
    h = c((ord(d[3])&0xff) << 24) | c((ord(d[2]) & 0xff) << 16) | \
            c((ord(d[1]) & 0xff) << 8) | c(ord(d[0]) & 0xff)
    return h


