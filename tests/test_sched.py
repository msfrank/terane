from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.task import deferLater
from terane.sched import Scheduler, STATE_READY

class Worker(object):
    def __init__(self):
        self.niterations = 10
    def next(self):
        if self.niterations == 0:
            raise StopIteration()
        self.niterations -= 1

class DeferredWorker(object):
    def __init__(self):
        self.niterations = 3
    def decrement(self):
        self.niterations -= 1
    def next(self):
        if self.niterations == 0:
            raise StopIteration()
        return deferLater(reactor, 1, self.decrement)

class GeneratorWorker(object):
    def __init__(self):
        self.niterations = 3
    def next(self):
        while self.niterations > 0:
            result = yield deferLater(reactor, 1, lambda: 42)
            if result != 42:
                raise Exception("yielded value is not 42")
            self.niterations -= 1
        raise StopIteration()

class WorkerFailed(Exception):
    pass

class WorkerFailure(object):
    def next(self):
        raise WorkerFailed()
 
class DeferredFailure(object):
    def failure(self):
        raise WorkerFailed()
    def next(self):
        return deferLater(reactor, 1, self.failure)
 
class GeneratorFailure(object):
    def next(self):
        result = yield deferLater(reactor, 1, lambda: 42)
        raise WorkerFailed()
 
class Scheduler_Tests(unittest.TestCase):

    def setUp(self):
        self.scheduler = Scheduler()

    def _taskDone(self, task, workers):
        self.assertEqual(task.state, STATE_READY)
        for worker in workers:
            self.assertEqual(worker.niterations, 0)

    def test_worker(self):
        task = self.scheduler.addTask('task 1')
        workers = [Worker()]
        task.addWorker(workers[0])
        return task.whenStateChanges().addCallback(self._taskDone, workers)

    def test_multiple_workers(self):
        task = self.scheduler.addTask('task 1')
        workers = [Worker(), Worker(), Worker()]
        task.addWorker(workers[0])
        task.addWorker(workers[1])
        task.addWorker(workers[2])
        return task.whenStateChanges().addCallback(self._taskDone, workers)

    def test_deferred_worker(self):
        task = self.scheduler.addTask('task 1')
        workers = [DeferredWorker()]
        task.addWorker(workers[0])
        return task.whenStateChanges().addCallback(self._taskDone, workers)

    def test_generator_worker(self):
        task = self.scheduler.addTask('task 1')
        workers = [GeneratorWorker()]
        task.addWorker(workers[0])
        return task.whenStateChanges().addCallback(self._taskDone, workers)

    def test_worker_fails(self):
        task = self.scheduler.addTask('task 1')
        return self.assertFailure(task.addWorker(WorkerFailure()).whenDone(), WorkerFailed)

    def test_deferred_fails(self):
        task = self.scheduler.addTask('task 1')
        return self.assertFailure(task.addWorker(DeferredFailure()).whenDone(), WorkerFailed)

    def test_generator_fails(self):
        task = self.scheduler.addTask('task 1')
        return self.assertFailure(task.addWorker(GeneratorFailure()).whenDone(), WorkerFailed)
