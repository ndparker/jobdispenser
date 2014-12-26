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
 Tests for wolfe.scheduler._job_queue
======================================

Tests for wolfe.scheduler._job_queue.
"""
from __future__ import with_statement, absolute_import

__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

from nose.tools import (
    assert_equals, assert_false, assert_true, assert_raises
)

from ..._util import Bunch

from wolfe.scheduler import _job_queue


# pylint: disable = protected-access


class Wrapper(object):
    """ Test wrsapper """

    def __init__(self, job):
        self.job = job

    def __lt__(self, other):
        return self.job.id > other.job.id


def test_job_queue_empty():
    """ Job queue starts empty and shows it """
    queue = _job_queue.JobQueue(None)

    assert_equals(len(queue._queue), 0)
    assert_false(queue)
    assert_equals(len(queue), 0)
    assert_false(1 in queue)


def test_job_queue_one_job():
    """ Job queue deals with one job """
    queue = _job_queue.JobQueue(Wrapper)

    queue.put(Bunch(id=2))
    assert_equals(len(queue._queue), 1)
    assert_true(queue)
    assert_equals(len(queue), 1)
    assert_false(1 in queue)
    assert_true(2 in queue)


def test_job_queue_more_jobs_peek_get():
    """ Job queue sorts properly """
    queue = _job_queue.JobQueue(Wrapper)

    queue.put(Bunch(id=2))
    queue.put(Bunch(id=3))
    assert_equals(len(queue._queue), 2)
    assert_true(queue)
    assert_equals(len(queue), 2)
    assert_false(1 in queue)
    assert_true(2 in queue)
    assert_true(3 in queue)
    assert_false(4 in queue)

    queue.put(Bunch(id=1))
    assert_true(1 in queue)

    assert_equals(queue.peek().job.id, 3)

    assert_equals(queue.get().id, 3)
    assert_false(3 in queue)
    assert_true(1 in queue)

    assert_equals(queue.get().id, 2)
    assert_equals(queue.get().id, 1)

    with assert_raises(IndexError):
        queue.get()


def test_job_queue_iter():
    """ Job queue provides iterator """
    queue = _job_queue.JobQueue(Wrapper)

    queue.put(Bunch(id=2))
    queue.put(Bunch(id=3))
    queue.put(Bunch(id=1))

    result = []
    for item in queue:
        result.append((
            item.id,
            [x.job.id for x in queue._queue],
            list(sorted(queue._ids)),
        ))

    assert_equals(result, [
        (3, [2, 1], [1, 2]),
        (2, [1], [1]),
        (1, [], []),
    ])
