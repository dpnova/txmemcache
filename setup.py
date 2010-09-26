#!/usr/bin/env python

# Try to use setuptools from http://peak.telecommunity.com/DevCenter/setuptools
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

VERSION = '0.1'

setup(
    author='Drew Smathers',
    author_email='drew dot smathers at gmail dot com',
    name='txmemcached',
    version=VERSION,
    install_requires=['zope.interface>=3.2.0','Twisted'],
    description="""Multi-client proxy with hashing algorithms for twisted.protocols.memcache""",
    license='MIT License',
    url='http://www.enterthefoo.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Topic :: System'],
    packages=[ 'txmemcache' ],
    package_dir={'txmemcache': 'txmemcache'},
    )
