# Copyright 2010,2011 Michael Frank <msfrank@syntaxjockey.com>
#
# This file is part of Terane.
#
# Terane is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Terane is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with Terane.  If not, see <http://www.gnu.org/licenses/>.

import re, time, email.utils, datetime
from terane.plugins import Plugin
from terane.filters import Filter, FilterError, StopFiltering
from terane.loggers import getLogger

logger = getLogger("terane.filters.datetime")

class DatetimeFilterPlugin(Plugin):

    def configure(self, section):
        pass

    def instance(self):
        return DatetimeFilter()


class DatetimeFilter(Filter):

    def configure(self, section):
        pass

    def infields(self):
        # this filter requires the following incoming fields
        return set()

    def outfields(self):
        # this filter guarantees values for the following outgoing fields
        return set(('ts',))

    def filter(self, fields):
        try:
            # if there is already a ts value, then skip parsing
            if 'ts' in fields:
                pass
            # parse the timestamp
            if 'rfc2822_date' in fields:
                ts = email.utils.parsedate_tz(fields['rfc2822_date'])
            elif 'syslog_date' in fields:
                date = fields['syslog_date']
                # if there is no leading zero in front of the day, then add it
                if date[4] == ' ':
                    date = list(date)
                    date[4] = '0'
                    date = ''.join(date)
                # append the year to the date string
                date += ' %i' % time.localtime()[0]
                # parse the date string into a struct_time
                ts = time.strptime(date, "%b %d %H:%M:%S %Y")
            else:
                year,mon,mday,hour,min,sec,wday,yday,isdst = time.localtime()
                if 'year' in fields:
                    year = time.strptime(fields['year'],"%Y")[0]
                if 'abbrev_year' in fields:
                    year = time.strptime(fields['abbrev_year'],"%y")[0]
                if 'month' in fields:
                    mon = time.strptime(fields['month'],"%m")[1]
                if 'month_name' in fields:
                    mon = time.strptime(fields['month_name'],"%B")[1]
                if 'abbrev_month_name' in fields:
                    mon = time.strptime(fields['abbrev_month_name'],"%b")[1]
                if 'day' in fields:
                    mday = time.strptime(fields['day'],"%d")[2]
                if 'hour' in fields:
                    hour = time.strptime(fields['hour'],"%H")[3]
                if '12hour' in fields and 'ampm' in fields:
                    hour = time.strptime(fields['12hour'] + fields['ampm'],"%I%p")[3]
                if 'minute' in fields:
                    minute = time.strptime(fields['minute'],"%M")[4]
                if 'second' in fields:
                    second = time.strptime(fields['second'],"%M")[5]
                if 'weekday' in fields:
                    wday = time.strptime(fields['weekday'],"%w")[6]
                if 'weekday_name' in fields:
                    wday = time.strptime(fields['weekday_name'],"%A")[6]
                if 'abbrev_weekday_name' in fields:
                    yday = time.strptime(fields['abbrev_weekday_name'],"%a")[6]
                if 'yearday' in fields:
                    yday = time.strptime(fields['yearday'],"%j")[7]
                ts = (year,mon,mday,hour,min,sec,wday,yday,-1)
        except Exception, e:
            raise FilterError("failed to generate ts: %s" % e)
        try:
            fields['ts'] = datetime.datetime.fromtimestamp(time.mktime(ts)).isoformat()
        except Exception, e:
            raise FilterError("failed to generate 'ts': %s" %  e)
        return fields
