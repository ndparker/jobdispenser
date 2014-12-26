# -*- coding: ascii -*-
u"""
:Copyright:

 Copyright 2014
 Andr\xe9 Malo or his licensors, as applicable

:License:

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

======================================
 Tests for wolfe.scheduler._scheduler
======================================

Tests for wolfe.scheduler._scheduler.
"""
from __future__ import absolute_import, with_statement

__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import itertools as _it
import operator as _op

from nose.tools import (
    assert_equals, assert_false, assert_true, assert_raises
)
import mock as _mock

from ..._util import mock, Bunch

from wolfe.scheduler import _scheduler


# pylint: disable = protected-access
# pylint: disable = missing-docstring
# pylint: disable = no-member


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_init(locks, job_queue, util, waiting):
    """ Scheduler properly initializes """
    util.DelayedJob = 'DELAYEDJOB'
    job_queue.JobQueue.side_effect = lambda x: ('JOBQUEUE', x)
    locks.Locks.side_effect = lambda x: ('LOCKS', x)
    waiting.Waiting.side_effect = lambda x: ('WAITING', x)

    scheduler = _scheduler.Scheduler("FINI")

    assert_equals(scheduler.__dict__, {
        'jobs': {},
        '_delayed': ('JOBQUEUE', 'DELAYEDJOB'),
        '_executing': {},
        '_executors': {},
        '_failed': set([]),
        '_finished': 'FINI',
        '_groups': {},
        '_locks': ('LOCKS', scheduler),
        '_waiting': ('WAITING', scheduler),
    })


@mock(_scheduler, '_locks')
@mock(_scheduler, '_job_queue')
@mock(_scheduler, '_util')
@mock(_scheduler, '_waiting')
@mock(_scheduler, '_job', name='job')
def test_scheduler_is_done(job):
    """ Scheduler.is_done returns correct info """
    job.last_job_id.side_effect = [0, 5, 5, 10, 15]

    scheduler = _scheduler.Scheduler("FINI")
    assert_false(scheduler.is_done(0))
    assert_false(scheduler.is_done(1))
    assert_true(scheduler.is_done(3))
    scheduler.jobs[3] = 'lala'
    assert_false(scheduler.is_done(3))
    assert_true(scheduler.is_done(10))
    assert_false(scheduler.is_done(16))


@mock(_scheduler, '_locks')
@mock(_scheduler, '_job_queue')
@mock(_scheduler, '_util')
@mock(_scheduler, '_waiting')
def test_scheduler_execution_attempt():
    """ Scheduler.execution_attempt returns attempt or None """
    scheduler = _scheduler.Scheduler("FINI")
    scheduler._executing = {8: 'EIGHT'}

    assert_equals(scheduler.execution_attempt(10), None)
    assert_equals(scheduler.execution_attempt(8), 'EIGHT')
    assert_equals(scheduler.execution_attempt(8), 'EIGHT')


@mock(_scheduler, '_locks')
@mock(_scheduler, '_job_queue')
@mock(_scheduler, '_util')
@mock(_scheduler, '_waiting')
@mock(_scheduler, '_group', name='group')
def test_scheduler_get_group(group):
    """ Scheduler.get_group creates a new group or returns an existing """
    group.Group.side_effect = lambda x, y, z: ['GROUP', x, y, z]
    scheduler = _scheduler.Scheduler("FINI")

    result1 = scheduler.get_group('lolo')
    assert_equals(result1, [
        'GROUP', 'lolo', scheduler._locks, scheduler
    ])
    assert_equals(map(tuple, group.mock_calls), [
        ('Group', ('lolo', scheduler._locks, scheduler), {}),
    ])

    result2 = scheduler.get_group('lolo')
    assert_true(result1 is result2)
    assert_equals(map(tuple, group.mock_calls), [
        ('Group', ('lolo', scheduler._locks, scheduler), {}),
    ])

    result3 = scheduler.get_group('xoxo')
    assert_equals(result3, [
        'GROUP', 'xoxo', scheduler._locks, scheduler
    ])
    assert_equals(map(tuple, group.mock_calls), [
        ('Group', ('lolo', scheduler._locks, scheduler), {}),
        ('Group', ('xoxo', scheduler._locks, scheduler), {}),
    ])


@mock(_scheduler, '_locks')
@mock(_scheduler, '_job_queue')
@mock(_scheduler, '_util')
@mock(_scheduler, '_waiting')
def test_scheduler_del_group():
    """ Scheduler.del_group works as documented """
    scheduler = _scheduler.Scheduler("FINI")

    scheduler._groups['x'] = []
    scheduler._groups['y'] = [1]
    assert_equals(scheduler._groups, {'x': [], 'y': [1]})

    scheduler.del_group('x')
    assert_equals(scheduler._groups, {'y': [1]})

    scheduler.del_group('x')
    assert_equals(scheduler._groups, {'y': [1]})

    scheduler.del_group('z')
    assert_equals(scheduler._groups, {'y': [1]})

    with assert_raises(AssertionError):
        scheduler.del_group('y')

    assert_equals(scheduler._groups, {'y': [1]})


@mock(_scheduler, '_locks')
@mock(_scheduler, '_job_queue')
@mock(_scheduler, '_util')
@mock(_scheduler, '_waiting')
@mock(_scheduler, '_job', name='job')
def test_scheduler_enter_todo(job):
    """ Scheduler.enter_todo unrolls todo graph and turns todos into jobs """
    entered = []

    class Scheduler(_scheduler.Scheduler):
        def _enter_job(self, job):
            entered.append(job)

    ids = _it.count(2).next

    job.joblist_from_todo.side_effect = lambda x: [
        Bunch(id=ids(), todo=x) for _ in xrange(2)
    ]

    scheduler = Scheduler("FINI")

    assert_equals(scheduler.enter_todo('t0d0'), 2)
    assert_equals(map(_op.attrgetter('id', 'todo'), entered), [
        (2, 't0d0'), (3, 't0d0'),
    ])


@mock(_scheduler, '_locks')
@mock(_scheduler, '_job_queue')
@mock(_scheduler, '_util')
@mock(_scheduler, '_waiting')
@mock(_scheduler, '_job', name='job')
def test_scheduler_enter_todo_cycle(job):
    """ Scheduler.enter_todo raises DependencyCycle """
    entered = []

    class Scheduler(_scheduler.Scheduler):
        def _enter_job(self, job):
            entered.append(job)

    job.joblist_from_todo.side_effect = [
        _scheduler.DependencyCycle(['a', 'b'])
    ]

    scheduler = Scheduler("FINI")

    try:
        scheduler.enter_todo('t0d0')
    except _scheduler.DependencyCycle, e:
        assert_equals(e.args, (['a', 'b'],))
    else:
        assert_false("DependencyCycle not raised")
    assert_equals(map(_op.attrgetter('id', 'todo'), entered), [])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_enter_job_timed(locks, job_queue, util, waiting):
    """ Scheduler._enter_job delays the job """
    util.DelayedJob = 'DELAYEDJOB'
    undelayed = []

    class Scheduler(_scheduler.Scheduler):
        def _enter_undelayed(self, job):
            undelayed.append(job)

    job = Bunch(id=25, not_before=2)
    scheduler = Scheduler('FINI')
    scheduler._enter_job(job)

    assert_equals(scheduler.jobs, {25: job})
    assert_equals(undelayed, [])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
        ('JobQueue().put', (job,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_enter_job_undelayed(locks, job_queue, util, waiting):
    """ Scheduler._enter_job passes job to next state """
    util.DelayedJob = 'DELAYEDJOB'
    undelayed = []

    class Scheduler(_scheduler.Scheduler):
        def _enter_undelayed(self, job):
            undelayed.append(job)

    job = Bunch(id=25, not_before=0)
    scheduler = Scheduler('FINI')

    scheduler._enter_job(job)

    assert_equals(scheduler.jobs, {25: job})
    assert_equals(undelayed, [job])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_enter_undelayed(locks, job_queue, util, waiting):
    """ Scheduler._enter_undelayed waits for other jobs """
    util.DelayedJob = 'DELAYEDJOB'
    independent = []

    class Scheduler(_scheduler.Scheduler):
        def _schedule_independent(self, job):
            independent.append(job)

    job = Bunch(id=25, not_before=0)
    scheduler = Scheduler('FINI')
    scheduler._waiting.put.side_effect = [True]

    scheduler._enter_undelayed(job)

    assert_equals(independent, [])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
        ('Waiting().put', (job,), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_enter_undelayed_schedule(locks, job_queue, util, waiting):
    """ Scheduler._enter_undelayed schedules a free job """
    util.DelayedJob = 'DELAYEDJOB'
    independent = []

    class Scheduler(_scheduler.Scheduler):
        def _schedule_independent(self, job):
            independent.append(job)

    job = Bunch(id=25, not_before=0)
    scheduler = Scheduler('FINI')
    scheduler._waiting.put.side_effect = [False]

    scheduler._enter_undelayed(job)

    assert_equals(independent, [job])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
        ('Waiting().put', (job,), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_schedule_independent(locks, job_queue, util, waiting):
    """ Scheduler._schedule_independent enqueues the job """
    util.DelayedJob = 'DELAYEDJOB'
    scheduled = []

    class Group(object):
        def __init__(self, name):
            self._name = name

        def schedule(self, job):
            scheduled.append(job)

    class Scheduler(_scheduler.Scheduler):
        def get_group(self, name):
            return Group(name)

    job = Bunch(id=25, not_before=0, group='lala')
    scheduler = Scheduler('FINI')

    scheduler._schedule_independent(job)

    assert_equals(scheduled, [job])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
        ('Locks().enter', (job,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
@mock(_scheduler, '_time', name='time')
def test_scheduler_undelay_jobs_future(locks, job_queue, util, waiting, time):
    """ Scheduler._undelay_jobs ignores future jobs """
    util.DelayedJob = 'DELAYEDJOB'

    class Scheduler(_scheduler.Scheduler):
        def _enter_undelayed(self, job):
            raise AssertionError("_enter_undelayed called")

    time.time.side_effect = [10.2]
    scheduler = Scheduler('FINI')
    scheduler._delayed.peek.side_effect = [Bunch(not_before=11)]

    scheduler._undelay_jobs()

    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
        ('JobQueue().__nonzero__', (), {}),
        ('JobQueue().__nonzero__', (), {}),
        ('JobQueue().peek', (), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
@mock(_scheduler, '_time', name='time')
def test_scheduler_undelay_jobs_past(locks, job_queue, util, waiting, time):
    """ Scheduler._undelay_jobs enters undelayed jobs """
    util.DelayedJob = 'DELAYEDJOB'
    undelayed = []

    class Scheduler(_scheduler.Scheduler):
        def _enter_undelayed(self, job):
            undelayed.append(job)

    job1 = Bunch(not_before=9)
    job2 = Bunch(not_before=10)

    time.time.side_effect = [10.2]
    scheduler = Scheduler('FINI')
    scheduler._delayed.__nonzero__.side_effect = [True, True, True, False]
    scheduler._delayed.peek.side_effect = [job1, job2]
    scheduler._delayed.get.side_effect = [job1, job2]

    scheduler._undelay_jobs()

    assert_equals(undelayed, [job1, job2])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
        ('JobQueue().__nonzero__', (), {}),
        ('JobQueue().__nonzero__', (), {}),
        ('JobQueue().peek', (), {}),
        ('JobQueue().get', (), {}),
        ('JobQueue().__nonzero__', (), {}),
        ('JobQueue().peek', (), {}),
        ('JobQueue().get', (), {}),
        ('JobQueue().__nonzero__', (), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
@mock(_scheduler, '_time', name='time')
def test_scheduler_undelay_jobs_both(locks, job_queue, util, waiting, time):
    """ Scheduler._undelay_jobs enters undelayed jobs and keeps futures """
    util.DelayedJob = 'DELAYEDJOB'
    undelayed = []

    class Scheduler(_scheduler.Scheduler):
        def _enter_undelayed(self, job):
            undelayed.append(job)

    job1 = Bunch(not_before=9)
    job2 = Bunch(not_before=10)
    job3 = Bunch(not_before=11)

    time.time.side_effect = [10.2]
    scheduler = Scheduler('FINI')
    scheduler._delayed.peek.side_effect = [job1, job2, job3]
    scheduler._delayed.get.side_effect = [job1, job2, job3]

    scheduler._undelay_jobs()

    assert_equals(undelayed, [job1, job2])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
        ('JobQueue().__nonzero__', (), {}),
        ('JobQueue().__nonzero__', (), {}),
        ('JobQueue().peek', (), {}),
        ('JobQueue().get', (), {}),
        ('JobQueue().__nonzero__', (), {}),
        ('JobQueue().peek', (), {}),
        ('JobQueue().get', (), {}),
        ('JobQueue().__nonzero__', (), {}),
        ('JobQueue().peek', (), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_unwait_jobs(locks, job_queue, util, waiting):
    """ Scheduler._unwait_jobs schedules freed jobs in proper order """
    util.DelayedJob = 'DELAYEDJOB'
    util.QueuedJob = 'QUEUEDJOB'
    independent = []

    class Scheduler(_scheduler.Scheduler):
        def is_done(self, job_id):
            return True

        def _schedule_independent(self, job):
            independent.append(job)

    job1 = Bunch(id=12)
    job2 = Bunch(id=13)
    job3 = Bunch(id=14)

    scheduler = Scheduler('FINI')
    scheduler._waiting.free.side_effect = [[job3, job1, job2]]
    job_queue.JobQueue().__iter__.side_effect = [iter([job1, job2, job3])]

    scheduler._unwait_jobs(10)

    assert_equals(independent, [job1, job2, job3])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
        ('Waiting().free', (10,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
        ('JobQueue', (), {}),
        ('JobQueue', ('QUEUEDJOB',), {}),
        ('JobQueue().put', (job3,), {}),
        ('JobQueue().put', (job1,), {}),
        ('JobQueue().put', (job2,), {}),
        ('JobQueue().__iter__', (), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_request_job(locks, job_queue, util, waiting):
    """ Scheduler.request_job returns the correct job """
    util.DelayedJob = 'DELAYEDJOB'
    undelayed = []

    class Scheduler(_scheduler.Scheduler):
        def _undelay_jobs(self):
            undelayed.append(1)

    class Job(Bunch):
        def __lt__(self, other):
            return self.order < other.order or self.job.id < other.job.id

    job1 = Job(order=6, job=Bunch(id=10))
    job2 = Job(order=5, job=Bunch(id=11))
    job3 = Job(order=7, job=Bunch(id=12))

    group1 = _mock.MagicMock()
    group1.peek.side_effect = [job1]
    group1.get.side_effect = [job1.job]

    group2 = _mock.MagicMock()
    group2.peek.side_effect = [job2]
    group2.get.side_effect = [job2.job]

    group3 = _mock.MagicMock()
    group3.peek.side_effect = [job3]
    group3.get.side_effect = [job3.job]

    scheduler = Scheduler('FINI')
    scheduler._groups.update(
        group1=group1,
        group2=group2,
        group3=group3,
    )
    result = scheduler.request_job(Bunch(
        groups=['group1', 'group2', 'group3'],
        uid='lala',
        attempt=lambda: 'ATT',
    ))

    assert_equals(result, job2.job)
    assert_equals(undelayed, [1])
    assert_equals(scheduler._executing, {11: 'ATT'})
    assert_equals(scheduler._executors, {'lala': 11})
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
    assert_equals(map(tuple, group1.mock_calls), [
        ('peek', (), {}),
    ])
    assert_equals(map(tuple, group2.mock_calls), [
        ('peek', (), {}),
        ('get', (), {}),
    ])
    assert_equals(map(tuple, group3.mock_calls), [
        ('peek', (), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
@mock(_scheduler, '_constants', name='const')
def test_scheduler_request_job_default(locks, job_queue, util, waiting,
                                       const):
    """ Scheduler.request_job falls back to default group """
    util.DelayedJob = 'DELAYEDJOB'
    undelayed = []

    class Scheduler(_scheduler.Scheduler):
        def _undelay_jobs(self):
            undelayed.append(1)

    class Job(Bunch):
        def __lt__(self, other):
            return self.order < other.order or self.job.id < other.job.id

    job1 = Job(order=6, job=Bunch(id=10))
    job2 = Job(order=5, job=Bunch(id=11))
    job3 = Job(order=7, job=Bunch(id=12))

    group1 = _mock.MagicMock()
    group1.peek.side_effect = [job1]
    group1.get.side_effect = [job1.job]

    group2 = _mock.MagicMock()
    group2.peek.side_effect = [job2]
    group2.get.side_effect = [job2.job]

    group3 = _mock.MagicMock()
    group3.peek.side_effect = [job3]
    group3.get.side_effect = [job3.job]

    scheduler = Scheduler('FINI')
    scheduler._groups.update(
        group1=group1,
        group2=group2,
        group3=group3,
    )
    const.Group.DEFAULT = 'group1'
    result = scheduler.request_job(
        Bunch(groups=[], uid='lala', attempt=lambda: 'ATT')
    )

    assert_equals(result, job1.job)
    assert_equals(undelayed, [1])
    assert_equals(scheduler._executing, {10: 'ATT'})
    assert_equals(scheduler._executors, {'lala': 10})
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
    assert_equals(map(tuple, group1.mock_calls), [
        ('peek', (), {}),
        ('get', (), {}),
    ])
    assert_equals(map(tuple, group2.mock_calls), [
    ])
    assert_equals(map(tuple, group3.mock_calls), [
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_request_job_again(locks, job_queue, util, waiting):
    """ Scheduler.request_job finds the same job again """
    util.DelayedJob = 'DELAYEDJOB'
    undelayed = []

    class Scheduler(_scheduler.Scheduler):
        def _undelay_jobs(self):
            undelayed.append(1)

    class Job(Bunch):
        def __lt__(self, other):
            return self.order < other.order or self.job.id < other.job.id

    job1 = Job(order=6, job=Bunch(id=10))
    job2 = Job(order=5, job=Bunch(id=11))
    job3 = Job(order=7, job=Bunch(id=12))

    group1 = _mock.MagicMock()
    group1.peek.side_effect = [job1]
    group1.get.side_effect = [job1.job]

    group2 = _mock.MagicMock()
    group2.peek.side_effect = [job2]
    group2.get.side_effect = [job2.job]

    group3 = _mock.MagicMock()
    group3.peek.side_effect = [job3]
    group3.get.side_effect = [job3.job]

    attempt = Bunch(executor='lala', which=1)
    attempt2 = Bunch(executor='lala', which=2)

    scheduler = Scheduler('FINI')
    scheduler.jobs.update({
        10: job1.job,
        11: job2.job,
        12: job3.job,
    })
    scheduler._groups.update(
        group1=group1,
        group2=group2,
        group3=group3,
    )
    result = scheduler.request_job(
        Bunch(groups=['group2'], uid='lala', attempt=lambda: attempt)
    )
    result2 = scheduler.request_job(
        Bunch(groups=['group2'], uid='lala', attempt=lambda: attempt2)
    )

    assert_true(result is result2)
    assert_equals(result, job2.job)
    assert_equals(undelayed, [1])
    assert_equals(scheduler._executing, {11: attempt})
    assert_equals(scheduler._executors, {'lala': 11})
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
    assert_equals(map(tuple, group1.mock_calls), [
    ])
    assert_equals(map(tuple, group2.mock_calls), [
        ('peek', (), {}),
        ('get', (), {}),
    ])
    assert_equals(map(tuple, group3.mock_calls), [
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_request_job_none_1(locks, job_queue, util, waiting):
    """ Scheduler.request_job returns None for unknown groups """
    util.DelayedJob = 'DELAYEDJOB'
    undelayed = []

    class Scheduler(_scheduler.Scheduler):
        def _undelay_jobs(self):
            undelayed.append(1)

    class Job(Bunch):
        def __lt__(self, other):
            return self.order < other.order or self.job.id < other.job.id

    job1 = Job(order=6, job=Bunch(id=10))
    job2 = Job(order=5, job=Bunch(id=11))
    job3 = Job(order=7, job=Bunch(id=12))

    group1 = _mock.MagicMock()
    group1.peek.side_effect = [job1]
    group1.get.side_effect = [job1.job]

    group2 = _mock.MagicMock()
    group2.peek.side_effect = [job2]
    group2.get.side_effect = [job2.job]

    group3 = _mock.MagicMock()
    group3.peek.side_effect = [job3]
    group3.get.side_effect = [job3.job]

    scheduler = Scheduler('FINI')
    scheduler._groups.update(
        group1=group1,
        group2=group2,
        group3=group3,
    )
    result = scheduler.request_job(
        Bunch(groups=['group4'], uid='lala', attempt=lambda: 'ATT')
    )

    assert_true(result is None)
    assert_equals(undelayed, [1])
    assert_equals(scheduler._executing, {})
    assert_equals(scheduler._executors, {})
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
    assert_equals(map(tuple, group1.mock_calls), [
    ])
    assert_equals(map(tuple, group2.mock_calls), [
    ])
    assert_equals(map(tuple, group3.mock_calls), [
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_request_job_none_2(locks, job_queue, util, waiting):
    """ Scheduler.request_job returns None for empty groups """
    util.DelayedJob = 'DELAYEDJOB'
    undelayed = []

    class Scheduler(_scheduler.Scheduler):
        def _undelay_jobs(self):
            undelayed.append(1)

    class Job(Bunch):
        def __lt__(self, other):
            return self.order < other.order or self.job.id < other.job.id

    job1 = Job(order=6, job=Bunch(id=10))
    job2 = Job(order=5, job=Bunch(id=11))
    job3 = Job(order=7, job=Bunch(id=12))

    group1 = _mock.MagicMock()
    group1.peek.side_effect = [job1]
    group1.get.side_effect = [job1.job]

    group2 = _mock.MagicMock()
    group2.peek.side_effect = [job2]
    group2.get.side_effect = [job2.job]

    group3 = _mock.MagicMock()
    group3.peek.side_effect = [job3]
    group3.get.side_effect = [job3.job]

    group4 = _mock.MagicMock()
    group4.peek.side_effect = lambda: None
    group4.get.side_effect = [IndexError]

    scheduler = Scheduler('FINI')
    scheduler._groups.update(
        group1=group1,
        group2=group2,
        group3=group3,
        group4=group4,
    )
    result = scheduler.request_job(
        Bunch(groups=['group4'], uid='lala', attempt=lambda: 'ATT')
    )

    assert_true(result is None)
    assert_equals(undelayed, [1])
    assert_equals(scheduler._executing, {})
    assert_equals(scheduler._executors, {})
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
    assert_equals(map(tuple, group1.mock_calls), [
    ])
    assert_equals(map(tuple, group2.mock_calls), [
    ])
    assert_equals(map(tuple, group3.mock_calls), [
    ])
    assert_equals(map(tuple, group4.mock_calls), [
        ('peek', (), {}),
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_finish_job(locks, job_queue, util, waiting):
    """ Scheduler.finish_job schedules finishes successful job """
    util.DelayedJob = 'DELAYEDJOB'
    util.QueuedJob = 'QUEUEDJOB'
    unwaited = []
    finished = []

    class Scheduler(_scheduler.Scheduler):
        def _unwait_jobs(self, finished_id):
            unwaited.append(finished_id)

    job = Bunch(id=56, attempts=[])
    job2 = Bunch(id=55)
    attempt = _mock.MagicMock()
    attempt.executor = 'lolo'
    attempt2 = _mock.MagicMock()
    attempt2.executor = 'lala'
    result = Bunch(failed=False)

    scheduler = Scheduler(Bunch(put=finished.append))
    scheduler.jobs[56] = job
    scheduler.jobs[55] = job2
    scheduler._executing[56] = attempt
    scheduler._executors['lolo'] = 56
    scheduler._executing[55] = attempt2
    scheduler._executors['lala'] = 55

    scheduler._locks.release.side_effect = lambda x: []
    job_queue.JobQueue().__iter__.side_effect = lambda: iter(())

    scheduler.finish_job(56, 12345, result)

    assert_equals(scheduler.jobs, {55: job2})
    assert_equals(scheduler._executing, {55: attempt2})
    assert_equals(scheduler._executors, {'lala': 55})
    assert_equals(scheduler._failed, set())
    assert_equals(finished, [job])
    assert_equals(unwaited, [56])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
        ('Locks().release', (job,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
        ('JobQueue', (), {}),
        ('JobQueue', ('QUEUEDJOB',), {}),
        ('JobQueue().__iter__', (), {}),
    ])
    assert_equals(map(tuple, attempt.mock_calls), [
        ('finish', (12345, result), {}),
    ])
    assert_equals(map(tuple, attempt2.mock_calls), [
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_finish_job_failed(locks, job_queue, util, waiting):
    """ Scheduler.finish_job schedules finishes failed job """
    util.DelayedJob = 'DELAYEDJOB'
    util.QueuedJob = 'QUEUEDJOB'
    unwaited = []
    finished = []
    failed = []

    class Scheduler(_scheduler.Scheduler):
        def _unwait_jobs(self, finished_id):
            unwaited.append(finished_id)

        def _fail_job(self, job):
            failed.append(job)

    job = Bunch(id=56, attempts=[])
    job2 = Bunch(id=55)
    attempt = _mock.MagicMock()
    attempt.executor = 'lolo'
    attempt2 = _mock.MagicMock()
    attempt2.executor = 'lala'
    result = Bunch(failed=True)

    scheduler = Scheduler(Bunch(put=finished.append))
    scheduler.jobs[56] = job
    scheduler.jobs[55] = job2
    scheduler._executing[56] = attempt
    scheduler._executors['lolo'] = 56
    scheduler._executing[55] = attempt2
    scheduler._executors['lala'] = 55

    scheduler._locks.release.side_effect = lambda x: []
    job_queue.JobQueue().__iter__.side_effect = lambda: iter(())

    scheduler.finish_job(56, 12345, result)

    assert_equals(scheduler.jobs, {56: job, 55: job2})
    assert_equals(scheduler._executing, {55: attempt2})
    assert_equals(scheduler._executors, {'lala': 55})
    assert_equals(failed, [job])
    assert_equals(finished, [])
    assert_equals(unwaited, [])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
        ('Locks().release', (job,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
        ('JobQueue', (), {}),
        ('JobQueue', ('QUEUEDJOB',), {}),
        ('JobQueue().__iter__', (), {}),
    ])
    assert_equals(map(tuple, attempt.mock_calls), [
        ('finish', (12345, result), {}),
    ])
    assert_equals(map(tuple, attempt2.mock_calls), [
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_finish_job_unlock(locks, job_queue, util, waiting):
    """ Scheduler.finish_job schedules unlocked jobs """
    util.DelayedJob = 'DELAYEDJOB'
    util.QueuedJob = 'QUEUEDJOB'
    unwaited = []
    finished = []
    scheduled = []
    failed = []

    class Group(object):
        def __init__(self, name):
            self._name = name

        def schedule(self, job):
            scheduled.append(job)

    class Scheduler(_scheduler.Scheduler):
        def get_group(self, name):
            return Group(name)

        def _unwait_jobs(self, finished_id):
            unwaited.append(finished_id)

        def _fail_job(self, job):
            failed.append(job)

    job = Bunch(id=56, attempts=[])
    job2 = Bunch(id=55)
    job3 = Bunch(id=57, group='lala')
    job4 = Bunch(id=58, group='lolo')
    attempt = _mock.MagicMock()
    attempt.executor = 'xxx'
    attempt2 = _mock.MagicMock()
    attempt2.executor = 'yyy'
    result = Bunch(failed=True)

    scheduler = Scheduler(Bunch(put=finished.append))
    scheduler.jobs[56] = job
    scheduler.jobs[55] = job2
    scheduler._executing[56] = attempt
    scheduler._executors['xxx'] = 56
    scheduler._executing[55] = attempt2
    scheduler._executors['yyy'] = 55

    scheduler._locks.release.side_effect = lambda x: [job3, job4]
    job_queue.JobQueue().__iter__.side_effect = lambda: iter((job4, job3))

    scheduler.finish_job(56, 12345, result)

    assert_equals(scheduler.jobs, {56: job, 55: job2})
    assert_equals(scheduler._executing, {55: attempt2})
    assert_equals(scheduler._executors, {'yyy': 55})
    assert_equals(failed, [job])
    assert_equals(finished, [])
    assert_equals(unwaited, [])
    assert_equals(scheduled, [job4, job3])
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
        ('Locks().release', (job,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
        ('JobQueue', (), {}),
        ('JobQueue', ('QUEUEDJOB',), {}),
        ('JobQueue().put', (job3,), {}),
        ('JobQueue().put', (job4,), {}),
        ('JobQueue().__iter__', (), {}),
    ])
    assert_equals(map(tuple, attempt.mock_calls), [
        ('finish', (12345, result), {}),
    ])
    assert_equals(map(tuple, attempt2.mock_calls), [
    ])


@mock(_scheduler, '_locks', name='locks')
@mock(_scheduler, '_job_queue', name='job_queue')
@mock(_scheduler, '_util', name='util')
@mock(_scheduler, '_waiting', name='waiting')
def test_scheduler_fail_job(locks, job_queue, util, waiting):
    """ Scheduler._fail_job adds job ID to failed set """
    util.DelayedJob = 'DELAYEDJOB'

    job = Bunch(id=23)
    scheduler = _scheduler.Scheduler('FINI')
    scheduler.jobs[23] = job
    scheduler._failed.add(12)

    scheduler._fail_job(job)

    assert_equals(scheduler._failed, set([12, 23]))
    assert_equals(map(tuple, locks.mock_calls), [
        ('Locks', (scheduler,), {}),
    ])
    assert_equals(map(tuple, waiting.mock_calls), [
        ('Waiting', (scheduler,), {}),
    ])
    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', ('DELAYEDJOB',), {}),
    ])
