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

==================================
 Tests for wolfe.scheduler._group
==================================

Tests for wolfe.scheduler._group.
"""
from __future__ import with_statement, absolute_import

__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

from nose.tools import (
    assert_equals, assert_false, assert_true, assert_raises
)
import mock as _mock

from ..._util import Bunch, mock

from wolfe.scheduler import _group


# pylint: disable = protected-access
# pylint: disable = no-member
# pylint: disable = pointless-statement
# pylint: disable = invalid-name
# pylint: disable = missing-docstring


class _Scheduler(object):
    """ Sample scheduler """
    just_me = id(_group)


@mock(_group, '_job_queue', name='job_queue')
@mock(_group, '_util', name='util')
def test_group_init(job_queue, util):
    """ Group initializes properly """
    scheduler = _Scheduler()
    util.QueuedJob = 'laber'
    queue = []
    job_queue.JobQueue.side_effect = lambda x: queue
    group = _group.Group('foo', 'locks', scheduler)

    assert_equals(group.name, 'foo')
    assert_equals(group._locks, 'locks')
    assert_true(queue is group._queue)
    assert_equals(map(tuple, job_queue.JobQueue.mock_calls), [
        ('', ('laber',), {}),
    ])
    assert_equals(group._scheduler.just_me, scheduler.just_me)
    assert_false(group)
    group._queue.append(1)
    assert_true(group)

    # scheduler should be weakref'd. Test that by deleting our reference:
    del scheduler
    with assert_raises(ReferenceError):
        group._scheduler.just_me


@mock(_group, '_job_queue', name='job_queue')
@mock(_group, '_util')
def test_group_schedule_false(job_queue):
    """ Group.schedule return false on locked jobs """
    job_queue.JobQueue.side_effect = lambda x: None
    scheduler = _mock.MagicMock()
    group = _group.Group('foo', 'locks', scheduler)

    assert_false(group.schedule(Bunch(locks_waiting=2)))


@mock(_group, '_job_queue', name='job_queue')
@mock(_group, '_util', name='util')
def test_group_schedule_true(job_queue, util):
    """ Group.schedule return true on scheduled jobs """
    scheduler = _mock.MagicMock()
    locks = _mock.MagicMock()
    locks.acquire.side_effect = [True]
    group = _group.Group('foo', locks, scheduler)

    job = Bunch(locks_waiting=0, id=23)
    assert_true(group.schedule(job))

    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', (util.QueuedJob,), {}),
        ('JobQueue().put', (job,), {}),
    ])

    assert_equals(map(tuple, scheduler.mock_calls), [
    ])

    assert_equals(map(tuple, locks.mock_calls), [
        ('acquire', (job,), {}),
    ])


@mock(_group, '_job_queue', name='job_queue')
@mock(_group, '_util', name='util')
def test_group_schedule_assert(job_queue, util):
    """ Group.schedule raises assertion error on inconsistent jobs """
    scheduler = _mock.MagicMock()
    locks = _mock.MagicMock()
    locks.acquire.side_effect = [False]
    group = _group.Group('foo', locks, scheduler)

    job = Bunch(locks_waiting=0, id=23)
    with assert_raises(AssertionError):
        group.schedule(job)

    assert_equals(map(tuple, job_queue.mock_calls), [
        ('JobQueue', (util.QueuedJob,), {}),
    ])

    assert_equals(map(tuple, scheduler.mock_calls), [
    ])

    assert_equals(map(tuple, locks.mock_calls), [
        ('acquire', (job,), {}),
    ])


@mock(_group, '_job_queue', name='job_queue')
@mock(_group, '_util')
def test_group_peek_empty(job_queue):
    """ Group.peek returns None if empty """
    queue = []
    job_queue.JobQueue.side_effect = lambda x: queue
    scheduler = _mock.MagicMock()
    group = _group.Group('foo', 'locks', scheduler)

    assert_equals(group.peek(), None)


@mock(_group, '_job_queue', name='job_queue')
@mock(_group, '_util')
def test_group_peek_something(job_queue):
    """ Group.peek returns the tip if not empty """
    class queue(list):
        def peek(self):
            return self[0]
    queue = queue([4])
    job_queue.JobQueue.side_effect = lambda x: queue
    scheduler = _mock.MagicMock()
    group = _group.Group('foo', 'locks', scheduler)

    assert_equals(group.peek(), 4)


@mock(_group, '_job_queue', name='job_queue')
@mock(_group, '_util')
def test_group_get_something(job_queue):
    " Group.get extracts the tip if not empty, raises IndexError otherwise "
    class queue(list):
        def get(self):
            return self.pop(0)
    queue = queue([4, 5])
    job_queue.JobQueue.side_effect = lambda x: queue
    scheduler = _mock.MagicMock()
    locks = _mock.MagicMock()
    group = _group.Group('foo', locks, scheduler)

    assert_equals(group.get(), 4)
    assert_equals(map(tuple, scheduler.mock_calls), [
    ])

    assert_equals(group.get(), 5)
    assert_equals(map(tuple, scheduler.mock_calls), [
        ('del_group', ('foo',), {}),
    ])

    with assert_raises(IndexError):
        group.get()
    assert_equals(map(tuple, scheduler.mock_calls), [
        ('del_group', ('foo',), {}),
        ('del_group', ('foo',), {}),
    ])
