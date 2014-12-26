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

====================================
 Tests for wolfe.scheduler._waiting
====================================

Tests for wolfe.scheduler._waiting.
"""
from __future__ import with_statement, absolute_import

__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import collections as _collections

from nose.tools import (
    assert_equals, assert_raises, assert_false, assert_true,
)
import mock as _mock

from ..._util import Bunch

from wolfe.scheduler import _waiting


# pylint: disable = protected-access
# pylint: disable = pointless-statement
# pylint: disable = no-member


class _Scheduler(object):
    """ Sample scheduler """
    just_me = id(_waiting)


def test_waiting_init():
    """ Waiting initializes properly """
    scheduler = _Scheduler()
    waiting = _waiting.Waiting(scheduler)

    assert_equals(waiting._scheduler.just_me, scheduler.just_me)
    assert_equals(type(waiting._waiting), set)
    assert_equals(type(waiting._waiting_for), _collections.defaultdict)
    assert_equals(waiting._waiting_for.default_factory, set)

    # scheduler should be weakref'd. Test that by deleting our reference:
    del scheduler
    with assert_raises(ReferenceError):
        waiting._scheduler.just_me


def test_waiting_put_false():
    """ Waiting.put return false if there's nothing to wait for """
    scheduler = _mock.MagicMock()
    scheduler.is_done.side_effect = lambda x: True
    job = Bunch(predecessors=[18, 20, 21])

    waiting = _waiting.Waiting(scheduler)

    assert_false(waiting.put(job))

    assert_equals(waiting._waiting, set())
    assert_equals(dict(waiting._waiting_for), {})
    assert_equals(map(tuple, scheduler.mock_calls), [
        ('is_done', (18,), {}),
        ('is_done', (20,), {}),
        ('is_done', (21,), {}),
    ])


def test_waiting_put_true():
    """ Waiting.put return true if there's something to wait for """
    scheduler = _mock.MagicMock()
    scheduler.is_done.side_effect = lambda x: x != 20
    job = Bunch(predecessors=[18, 20, 21], id=24)

    waiting = _waiting.Waiting(scheduler)

    assert_true(waiting.put(job))

    assert_equals(waiting._waiting, set([24]))
    assert_equals(dict(waiting._waiting_for), {20: set([24])})
    assert_equals(map(tuple, scheduler.mock_calls), [
        ('is_done', (18,), {}),
        ('is_done', (20,), {}),
        ('is_done', (21,), {}),
    ])


def test_waiting_free():
    """ Waiting.free frees jobs and returns a list of them """
    scheduler = _mock.MagicMock()
    scheduler.is_done.side_effect = lambda x: x not in (20, 22)
    job = Bunch(predecessors=[18, 20, 21], id=24)
    job2 = Bunch(predecessors=[18, 20, 22], id=25)
    scheduler.jobs = {24: job, 25: job2}

    waiting = _waiting.Waiting(scheduler)

    assert_true(waiting.put(job))
    assert_true(waiting.put(job2))

    assert_equals(job.predecessors_waiting, 1)
    assert_equals(job2.predecessors_waiting, 2)

    assert_equals(waiting._waiting, set([24, 25]))
    assert_equals(dict(waiting._waiting_for), {
        20: set([24, 25]), 22: set([25])
    })

    assert_equals(waiting.free(17), [])
    assert_equals(waiting.free(18), [])
    assert_equals(waiting.free(20), [job])

    assert_equals(waiting._waiting, set([25]))
    assert_equals(dict(waiting._waiting_for), {22: set([25])})

    assert_equals(waiting.free(22), [job2])

    assert_equals(waiting._waiting, set())
    assert_equals(dict(waiting._waiting_for), {})

    assert_equals(map(tuple, scheduler.mock_calls), [
        ('is_done', (18,), {}),
        ('is_done', (20,), {}),
        ('is_done', (21,), {}),
        ('is_done', (18,), {}),
        ('is_done', (20,), {}),
        ('is_done', (22,), {}),
    ])
