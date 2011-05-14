#!/usr/bin/env python

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, Extension, find_packages

# jump through some hoops to get access to versionstring()
import sys, os
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
        'terane.commands',
        'terane.commands.search',
        'terane.commands.server',
        'terane.commands.tail',
        'terane.db',
        'terane.filters',
        'terane.inputs',
        'terane.lib',
        'terane.lib.whoosh',
        'terane.lib.whoosh',
        'terane.lib.whoosh.filedb',
        'terane.lib.whoosh.lang',
        'terane.lib.whoosh.support',
        'terane.lib.whoosh.qparser',
        'terane.outputs',
        ],
    ext_modules=[
        Extension('terane.db.storage', [
            'terane/db/storage.c',
            'terane/db/storage-did.c',
            'terane/db/storage-env.c',
            'terane/db/storage-iter.c',
            'terane/db/storage-logfd.c',
            'terane/db/storage-segment.c',
            'terane/db/storage-segment-doc.c',
            'terane/db/storage-segment-field.c',
            'terane/db/storage-segment-word.c',
            'terane/db/storage-toc.c',
            'terane/db/storage-txn.c',
            ],
            libraries=['db-4.8',],
            # turn off optimization for better stack traces
            extra_compile_args=['-O0']
            )
        ],
    entry_points={
        'console_scripts': [
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
            'messages=terane.filters.messages:MessagesFilterPlugin',
            'regex=terane.filters.regex:RegexFilterPlugin',
            'datetime=terane.filters.datetime:DatetimeFilterPlugin',
            'apache_combined=terane.filters.apache:ApacheCombinedFilterPlugin',
            'apache_common=terane.filters.apache:ApacheCommonFilterPlugin',
            'mysql_server=terane.filters.mysql:MysqlServerFilterPlugin',
            ],
        },
)
