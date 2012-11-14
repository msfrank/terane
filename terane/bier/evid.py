# Copyright 2010,2011,2012 Michael Frank <msfrank@syntaxjockey.com>
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

import time, datetime, dateutil.tz, calendar

TS_MIN = 0
TS_MAX = 2**32 - 1

OFFSET_MIN = 1
OFFSET_MAX = 2**64 - 1

class EVID(object):
    """
    An EVID (event identifier) uniquely identifies an event, and also
    provides a stable temporal ordering.
    """

    def __init__(self, ts, offset):
        if ts > TS_MAX:
            raise OverflowError("ts is out of range")
        if offset > OFFSET_MAX:
            raise OverflowError("offset is out of range")
        self.ts = int(ts)
        self.offset = int(offset)

    @classmethod
    def fromDatetime(cls, ts=datetime.datetime.now(), offset=0):
        """
        Create a new EVID object from the specified timestamp and offset.

        :param ts: The timestamp.
        :type ts: datetime.datetime
        :param offset: The offset.
        :type offset: int
        :returns: The new EVID object.
        :rtype: :class:`terane.bier.evid.EVID`
        """
        if not isinstance(ts, datetime.datetime):
            raise TypeError("ts must be a datetime.datetime")
        # if no timezone is specified, then assume local tz
        if ts.tzinfo == None:
            ts = ts.replace(tzinfo=dateutil.tz.tzlocal())
        # convert to UTC, if necessary
        if not ts.tzinfo == dateutil.tz.tzutc():
            ts = ts.astimezone(dateutil.tz.tzutc())
        return EVID(int(calendar.timegm(ts.timetuple())), offset)

    @classmethod
    def fromEvent(cls, event):
        """
        Create a new EVID object based on the timestamp and offset of the 
        specified Event.

        :param event: The event..
        :type event: :class:`terane.bier.event.Event`
        :returns: The new EVID object.
        :rtype: :class:`terane.bier.evid.EVID`
        """
        return EVID(int(calendar.timegm(event.ts.timetuple())), event.offset)

    def __str__(self):
        return "%i:%i" % (self.ts, self.offset)

    def __eq__(self, other):
        if not isinstance(other, EVID):
            return False
        if self.__cmp__(other) == 0:
            return True
        return False

    def __ne__(self, other):
        if not isinstance(other, EVID):
            return True
        if self.__cmp__(other) == 0:
            return False
        return True

    def __cmp__(self, other):
        return cmp((self.ts,self.offset), (other.ts,other.offset))

    def __add__(self, other):
        ts = self.ts + other.ts
        offset = self.offset + other.offset
        if offset > OFFSET_MAX:
            offset -= OFFSET_MAX
            ts += 1
        if ts > TS_MAX:
            raise OverflowError()
        return EVID(ts, offset)

    def __sub__(self, other):
        ts = self.ts - other.ts
        offset = self.offset - other.offset
        if offset < 0:
            offset = OFFSET_MAX + offset
            ts -= 1
        if ts < 0:
            raise OverflowError()
        return EVID(ts, offset)

EVID_MIN = EVID(0, 0)
EVID_MAX = EVID(TS_MAX, OFFSET_MAX)
