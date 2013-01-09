#!/usr/bin/env python

import sys, os

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, Extension, find_packages

# jump through some hoops to get access to versionstring()
from os.path import abspath, dirname
sys.path.insert(0, abspath(dirname(__file__)))
from terane import versionstring

# set extension module build parameters
extra_include_dirs = []
if 'TERANE_EXTRA_INCLUDE_PATH' in os.environ:
    extra_include_dirs = os.environ['TERANE_EXTRA_INCLUDE_PATH'].split(':')
extra_library_dirs = []
if 'TERANE_EXTRA_LIBRARY_PATH' in os.environ:
    extra_library_dirs = os.environ['TERANE_EXTRA_LIBRARY_PATH'].split(':')
extra_runtime_dirs = []
if 'TERANE_EXTRA_RUNTIME_PATH' in os.environ:
    extra_runtime_dirs = os.environ['TERANE_EXTRA_RUNTIME_PATH'].split(':')

setup(
    # package description
    name = "Terane",
    version = versionstring(),
    description="Distributed event indexing and search",
    author="Michael Frank",
    author_email="msfrank@syntaxockey.com",
    # installation dependencies
    install_requires=[
        "Twisted >= 10.1",
        "pyparsing",
        "python-dateutil",
        "urwid",
        "zope.interface",
        "zope.component",
        ],
    # package classifiers for PyPI
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
        'terane.auth',
        'terane.bier',
        'terane.bier.ql',
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
        ],
    ext_modules=[
        Extension('terane.outputs.store.backend', [
            'terane/outputs/store/backend.c',
            'terane/outputs/store/backend-env.c',
            'terane/outputs/store/backend-index.c',
            'terane/outputs/store/backend-index-field.c',
            'terane/outputs/store/backend-index-meta.c',
            'terane/outputs/store/backend-index-segment.c',
            'terane/outputs/store/backend-iter.c',
            'terane/outputs/store/backend-logfd.c',
            'terane/outputs/store/backend-msgpack-cmp.c',
            'terane/outputs/store/backend-msgpack-dump.c',
            'terane/outputs/store/backend-msgpack-load.c',
            'terane/outputs/store/backend-segment.c',
            'terane/outputs/store/backend-segment-event.c',
            'terane/outputs/store/backend-segment-field.c',
            'terane/outputs/store/backend-segment-meta.c',
            'terane/outputs/store/backend-segment-posting.c',
            'terane/outputs/store/backend-segment-term.c',
            'terane/outputs/store/backend-txn.c',
            ],
            # link against libdb
            libraries=['db'],
            # set search paths for headers and libraries
            include_dirs=extra_include_dirs,
            library_dirs=extra_library_dirs,
            runtime_library_dirs=extra_runtime_dirs,
            # turn off optimization for better stack traces
            extra_compile_args=['-O0', '-Wall']
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
        'terane.plugin': [
            # protocol plugins
            'protocol:xmlrpc=terane.protocols.xmlrpc:XMLRPCProtocolPlugin',
            # input plugins
            'input:file=terane.inputs.file:FileInputPlugin',
            'input:syslog=terane.inputs.syslog:SyslogInputPlugin',
            'input:collect=terane.inputs.collect:CollectInputPlugin',
            # filter plugins
            'filter:syslog=terane.filters.syslog:SyslogFilterPlugin',
            'filter:regex=terane.filters.regex:RegexFilterPlugin',
            'filter:dt=terane.filters.dt:DatetimeFilterPlugin',
            'filter:apache=terane.filters.apache:ApacheFilterPlugin',
            'filter:mysql=terane.filters.mysql:MysqlFilterPlugin',
            'filter:nagios=terane.filters.nagios:NagiosFilterPlugin',
            # output plugins
            'output:store=terane.outputs.store:StoreOutputPlugin',
            'output:forward=terane.outputs.forward:ForwardOutputPlugin',
            # field plugins
            'field:base=terane.bier.fields:BaseFieldPlugin',
            ],
        },
    test_suite="tests",
    tests_require=["setuptools_trial"]
)
