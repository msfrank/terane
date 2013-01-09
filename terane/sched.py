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
from types import GeneratorType
from zope.interface import Interface, implements
from twisted.internet.task import Cooperator
from twisted.internet.defer import Deferred

class IScheduler(Interface):
    def addTask(name, nice):
        """
        Create a new Task and add it to the Scheduler.
        """
    def removeTask(task):
        """
        Remove the specified task from the Scheduler.
        """
    def iterTasks():
        """
        Returns an iterator which yields each Task in the Scheduler.
        """

class Scheduler(object):
    """
    The toplevel scheduler which manages Tasks and dispatches work
    using a round-robin algorithm.
    """

    implements(IScheduler)

    def __init__(self, timeslice=0.01, epsilon=0.00000001, reactor=None):
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
        self._tasks = set()
        if reactor == None:
            from twisted.internet import reactor as reactor_
            self._reactor = reactor_
        else:
            self._reactor = reactor

    def _invoke(self, callable_, *args, **kwds):
        return self._reactor.callLater(self._epsilon, callable_, *args, **kwds)

    def addTask(self, name='', nice=1.0):
        """
        Create a new Task and add it to the Scheduler.

        :param name: The name.  Useful for identifying a task's purpose.
        :type name: str
        :param nice: The timeslice multiplier.
        :type nice: float
        :returns: A new Task object.
        :rtype: :class:`terane.sched.Task`
        """
        task = Task(self, name, nice)
        self._tasks.add(task)
        return task

    def iterTasks(self):
        """
        Returns an iterator enumerating each Task registered with
        the scheduler.

        :returns: An iterator enumerating :class:`terane.sched.Task` objects.
        :rtype: iter
        """
        return iter(self._tasks)

STATE_READY = 0
STATE_RUNNING = 1
STATE_WAITING = 2
STATE_STOPPED = 3
STATE_DONE = 4

class Governor(object):

    def __init__(self, task, sched):
        self._end = time() + (sched.timeslice * task.nice)

    def __call__(self):
        return time() >= self._end

class Task(object):
    
    def __init__(self, sched, name, nice):
        self._sched = sched
        self._cooperator = Cooperator(self._check, self._schedule)
        self._whenStateChanges = None
        self.name = name
        self.nice = nice
        self.runningworkers = 0
        self.completedworkers = 0
        self.runningtime = 0.0
        self.waitingtime = 0.0
        self.state = STATE_READY
        self.started = time()

    def _check(self):
        return Governor(self, self._sched)

    def _schedule(self, callable_):
        return self._sched._invoke(callable_)

    def _changeState(self, state):
        if state == self.state:
            return
        self.state = state
        if self._whenStateChanges != None:
            d = self._whenStateChanges
            self._whenStateChanges = None
            d.callback(self)

    def addWorker(self, iterator):
        """
        Adds a worker to the Task.  The worker needs to conform to the
        iterator interface.

        :param iterator: the worker which is executed a step at a time.
        :type iterator: iter
        :returns: A new Worker.
        :rtype: :class:`terane.sched.Worker`
        """
        worker = Worker(self, iterator)
        self.runningworkers += 1
        if self.state == STATE_READY:
            self._changeState(STATE_RUNNING)
        worker.whenDone().addCallbacks(self._taskDone, self._taskError)
        return worker

    def whenStateChanges(self):
        """
        Returns a Deferred which will fire when the state of the Task changes.
        The result passed to the Deferred is the Task.  The new state can be
        retrieved from the Task.state attribute.

        :returns: The Deferred.
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        if self._whenStateChanges == None:
            self._whenStateChanges = Deferred()
        return self._whenStateChanges

    def _taskDone(self, result):
        self.completedworkers += 1
        self.runningworkers -= 1
        if self.runningworkers == 0:
            self._changeState(STATE_READY)
        return result

    def _taskError(self, failure):
        self.completedworkers += 1
        self.runningworkers -= 1
        if self.runningworkers == 0:
            self._changeState(STATE_READY)
        return failure

class Worker(object):
    """
    """

    def __init__(self, task, iterable):
        self._task = task
        self._iterable = iterable
        self._waitResume = None
        self._waitingStart = None
        self._ctask = task._cooperator.cooperate(self)
        self._ctask.whenDone().addCallbacks(self._workerDone, self._workerError)
        self._whenDone = None
        self.iterable = iterable
        self.state = STATE_RUNNING

    def whenDone(self):
        """
        Returns a Deferred which will fire when the Worker is finished.  The
        result passed to the Deferred is the Worker.

        :returns: The Deferred.
        :rtype: :class:`twisted.internet.defer.Deferred`
        """
        if self._whenDone == None:
            self._whenDone = Deferred()
        return self._whenDone

    def __iter__(self):
        return self

    def next(self):
        if self._waitResume != None:
            toResume = self._waitResume
            self._waitResume = None
            return self._run(toResume)
        else:
            return self._run(self._iterable.next)

    def _run(self, op):
        caught = None
        runningStart = time()
        try:
            result = op()
            if isinstance(result, GeneratorType):
                self._iterable = result
                result = self._iterable.next()
        except Exception, caught:
            pass
        runningEnd = time()
        self._task.runningtime += runningEnd - runningStart
        if not caught == None:
            raise caught
        if isinstance(result, Deferred):
            self._waitingStart = runningEnd
            self.state = STATE_WAITING
            result.addCallbacks(self._waitDone, self._waitError)
        return result

    def _waitDone(self, result):
        waitingEnd = time()
        self._task.waitingtime += waitingEnd - self._waitingStart
        self._waitingStart = None
        self.state = STATE_RUNNING
        if isinstance(self._iterable, GeneratorType):
            self._waitResume = lambda: self._iterable.send(result)
        else:
            return result

    def _waitError(self, failure):
        waitingEnd = time()
        self._task.waitingtime += waitingEnd - self._waitingStart
        self._waitingStart = None
        self.state = STATE_RUNNING
        if isinstance(self._iterable, GeneratorType):
            self._waitResume = lambda: self._iterable.throw(failure.value)
        else:
            return failure

    def _workerDone(self, result):
        if self._waitingStart:
            waitingEnd = time()
            self._task.waitingtime += waitingEnd - self._waitingStart
            self._waitingStart = None
        self.state = STATE_DONE
        if self._whenDone != None:
            self._whenDone.callback(self)

    def _workerError(self, failure):
        if self._waitingStart:
            waitingEnd = time()
            self._task.waitingtime += waitingEnd - self._waitingStart
            self._waitingStart = None
        self.state = STATE_DONE
        if self._whenDone != None:
            self._whenDone.errback(failure)
