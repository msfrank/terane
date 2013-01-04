# Copyright 2012 Michael Frank <msfrank@syntaxjockey.com>
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

from time import time
from twisted.internet.task import Cooperator
from twisted.internet.defer import Deferred

class Scheduler(object):
    """
    The toplevel scheduler which manages Coroutines and dispatches work
    using a round-robin algorithm.
    """

    def __init__(self, timeslice=0.01, epsilon=0.00000001, reactor_=None):
        """
        :param timeslice: The amount of time in seconds given to each Coroutine.
        :type timeslice: float
        :param epsilon: How soon to schedule a work unit.
        :type epsilon: float
        :param reactor: The twisted reactor to use for scheduling.
        :type reactor:
        """
        self.timeslice = timeslice
        self._epsilon = epsilon
        self._coroutines = set()
        if reactor_ == None:
            from twisted.internet import reactor
            self._reactor = reactor
        else:
            self._reactor = reactor_

    def _invoke(self, callable_, *args, **kwds):
        return self._reactor.callLater(self._epsilon, callable_, *args, **kwds)

    def addCoroutine(self, title='', nice=1.0):
        """
        Create a new Coroutine and add it to the Scheduler.

        :param title: The title.  Useful for identifying a coroutine's purpose.
        :type title: str
        :param nice: The timeslice multiplier.
        :type nice: float
        :returns: A new Coroutine object.
        :rtype: :class:`terane.sched.Coroutine`
        """
        co = Coroutine(self, title, nice)
        self._coroutines.add(co)
        return co

    def iterCoroutines(self):
        """
        Returns an iterator enumerating each Coroutine registered with
        the scheduler.

        :returns: An iterator enumerating :class:`terane.sched.Coroutine` objects.
        :rtype: iter
        """
        return iter(self._coroutines)

STATE_READY = 0
STATE_RUNNING = 1
STATE_WAITING = 2
STATE_STOPPED = 3
STATE_DONE = 4

class Governor(object):

    def __init__(self, co, sched):
        self._end = time() + (sched.timeslice * co.nice)

    def __call__(self):
        return time() >= self._end

class Coroutine(object):
    
    def __init__(self, sched, title, nice):
        self._sched = sched
        self._cooperator = Cooperator(self._check, self._schedule)
        self.title = title
        self.nice = nice
        self.runningtasks = 0
        self.completedtasks = 0
        self.runningtime = 0.0
        self.waitingtime = 0.0
        self.state = STATE_READY
        self.started = time()

    def _check(self):
        return Governor(self, self._sched)

    def _schedule(self, callable_):
        return self._sched._invoke(callable_)

    def addTask(self, iterator):
        """
        Returns a Task object.
        """
        task = Task(self, iterator)
        self.runningtasks += 1
        if self.state == STATE_READY:
            self.state = STATE_RUNNING
        task.done.addCallbacks(self._taskDone, self._taskError)
        return task

    def _taskDone(self, result):
        self.completedtasks += 1
        self.runningtasks -= 1
        if self.runningtasks == 0:
            self.state = STATE_READY
        return result

    def _taskError(self, failure):
        self.completedtasks += 1
        self.runningtasks -= 1
        if self.runningtasks == 0:
            self.state = STATE_READY
        return failure

class Task(object):

    def __init__(self, co, iterator):
        self._co = co
        self._iterator = iterator
        self._waiting_start = None
        self._task = co._cooperator.cooperate(self)
        self.done = self._task.whenDone().addCallbacks(self._taskDone, self._taskError)
        self.state = STATE_RUNNING

    def __iter__(self):
        return self

    def next(self):
        caught = None
        running_start = time()
        try:
            result = self._iterator.next()
        except Exception, caught:
            pass
        running_end = time()
        self._co.runningtime += running_end - running_start
        if not caught == None:
            raise caught
        if isinstance(result, Deferred):
            self._waiting_start = running_end
            self.state = STATE_WAITING
            result.addCallbacks(self._waitDone, self._waitError)
        return result

    def _waitDone(self, result):
        waiting_end = time()
        self._co.waitingtime += waiting_end - self._waiting_start
        self._waiting_start = None
        self.state = STATE_RUNNING
        return result

    def _waitError(self, failure):
        waiting_end = time()
        self._co.waitingtime += waiting_end - self._waiting_start
        self.state = STATE_DONE
        return failure

    def _taskDone(self, result):
        if self._waiting_start:
            waiting_end = time()
            self._co.waitingtime += waiting_end - self._waiting_start
            self._waiting_start = None
        self.state = STATE_DONE
        return result

    def _taskError(self, failure):
        if self._waiting_start:
            waiting_end = time()
            self._co.waitingtime += waiting_end - self._waiting_start
            self._waiting_start = None
        self.state = STATE_DONE
        return failure
