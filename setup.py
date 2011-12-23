#!/usr/bin/env python

import sys, os

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, Extension, find_packages

# verify required dependencies are installed
try:
    import twisted, pyparsing, dateutil, urwid, zope.interface
except ImportError, e:
    print "Missing required dependency: %s" % e
    sys.exit(1)

# jump through some hoops to get access to versionstring()
from os.path import abspath, dirname
sys.path.insert(0, abspath(dirname(__file__)))
from terane import versionstring

setup(
    # package description
    name = "Terane",
    version = versionstring(),
    description="Distributed Log Search",
    author="Michael Frank",
    author_email="msfrank@syntaxockey.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment "" No Input/Output (Daemon)",
        "Framework :: Twisted",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: Other/Proprietary License",
        "Natural Language :: English",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: C",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Topic :: System :: Logging",
        "Topic :: Text Processing :: Indexing",
        ],
    # package contents
    packages=[
        'terane',
        'terane.bier',
        'terane.commands',
        'terane.commands.console',
        'terane.commands.grok',
        'terane.commands.search',
        'terane.commands.server',
        'terane.commands.tail',
        'terane.filters',
        'terane.inputs',
        'terane.outputs',
        'terane.outputs.store',
        'terane.protocols',
        'terane.query',
        ],
    ext_modules=[
        Extension('terane.outputs.store.backend', [
            'terane/outputs/store/backend.c',
            'terane/outputs/store/backend-did.c',
            'terane/outputs/store/backend-env.c',
            'terane/outputs/store/backend-index.c',
            'terane/outputs/store/backend-index-field.c',
            'terane/outputs/store/backend-index-meta.c',
            'terane/outputs/store/backend-index-segment.c',
            'terane/outputs/store/backend-iter.c',
            'terane/outputs/store/backend-logfd.c',
            'terane/outputs/store/backend-segment.c',
            'terane/outputs/store/backend-segment-doc.c',
            'terane/outputs/store/backend-segment-field.c',
            'terane/outputs/store/backend-segment-meta.c',
            'terane/outputs/store/backend-segment-word.c',
            'terane/outputs/store/backend-txn.c',
            ],
            libraries=['db-4.8',],
            # turn off optimization for better stack traces
            extra_compile_args=['-O0']
            )
        ],
    entry_points={
        'console_scripts': [
            'terane=terane.commands.console:console_main',
            'terane-grok=terane.commands.grok:grok_main',
            'terane-server=terane.commands.server:server_main',
            'terane-search=terane.commands.search:search_main',
            'terane-tail=terane.commands.tail:tail_main',
            ],
        'terane.plugin.input': [
            'file=terane.inputs.file:FileInputPlugin',
            'syslog=terane.inputs.syslog:SyslogInputPlugin',
            'collect=terane.inputs.collect:CollectInputPlugin',
            ],
        'terane.plugin.output': [
            'store=terane.outputs.store:StoreOutputPlugin',
            'forward=terane.outputs.forward:ForwardOutputPlugin',
            ],
        'terane.plugin.filter': [
            'syslog=terane.filters.syslog:SyslogFilterPlugin',
            'regex=terane.filters.regex:RegexFilterPlugin',
            'dt=terane.filters.dt:DatetimeFilterPlugin',
            'apache_combined=terane.filters.apache:ApacheCombinedFilterPlugin',
            'apache_common=terane.filters.apache:ApacheCommonFilterPlugin',
            'mysql_server=terane.filters.mysql:MysqlServerFilterPlugin',
            'nagios=terane.filters.nagios:NagiosFilterPlugin',
            ],
        'terane.plugin.protocol': [
            'xmlrpc=terane.protocols.xmlrpc:XMLRPCProtocolPlugin',
            ],
        },
)
