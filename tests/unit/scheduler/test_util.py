# -*- coding: ascii -*-
r"""
:Copyright:

 Copyright 2014 - 2016
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

=================================
 Tests for wolfe.scheduler._util
=================================

Tests for wolfe.scheduler._util.
"""
if __doc__:  # pragma: no cover
    # pylint: disable = redefined-builtin
    __doc__ = __doc__.encode('ascii').decode('unicode_escape')
__author__ = r"Andr\xe9 Malo".encode('ascii').decode('unicode_escape')
__docformat__ = "restructuredtext en"

import datetime as _dt

from nose.tools import assert_equals, assert_raises, assert_true
from ... import _util as _test

from wolfe.scheduler import _util

# pylint: disable = missing-docstring
# pylint: disable = invalid-name
# pylint: disable = unused-argument
# pylint: disable = protected-access


def test_scheduled_time():
    class Job(object):
        def __init__(self, not_before):
            self.not_before = not_before

    class tzinfo(_dt.tzinfo):
        def utcoffset(self, dt):
            return _dt.timedelta(0)

    _util._time = _test.mock.Mock()
    _util._time.time.side_effect = iter([20, 21, 22, 23, 24, 25])
    _util._dt = _test.mock.Mock()
    _util._dt.datetime.utcnow.return_value = _dt.datetime(2006, 12, 6, 16, 30)
    _util._pytz = None

    assert_equals(_util.scheduled_time(Job(0)), 20)
    assert_equals(_util.scheduled_time(Job(6)), 27)

    assert_equals(
        _util.scheduled_time(Job(_dt.datetime(2006, 12, 6, 12))), 22
    )
    assert_equals(
        _util.scheduled_time(Job(_dt.datetime(2006, 12, 6, 17))), 1823
    )

    with assert_raises(RuntimeError):
        _util.scheduled_time(
            Job(_dt.datetime(2006, 12, 6, 17, tzinfo=tzinfo()))
        )

    def localize(dto):
        return (dto - _dt.timedelta(hours=1)).replace(tzinfo=tzinfo())
    _util._pytz = _test.mock.Mock()
    _util._pytz.UTC.localize.side_effect = localize

    assert_equals(
        _util.scheduled_time(Job(
            _dt.datetime(2006, 12, 6, 17, tzinfo=tzinfo())
        )),
        5425
    )


def test_queued_job():
    class Job(object):
        def __init__(self, job_id, importance):
            self.id = job_id
            self.importance = importance

    queued1 = _util.QueuedJob(Job(1, 2))
    queued2 = _util.QueuedJob(Job(2, 1))
    queued3 = _util.QueuedJob(Job(3, 3))
    assert_true(queued1 < queued2)
    assert_true(queued3 < queued1)
    assert_true(queued3 < queued2)


def test_delayed_job():
    class Job(object):
        def __init__(self, not_before):
            self.not_before = not_before

    with _test.patched(_util, 'scheduled_time') as mock:
        mock.side_effect = lambda x: x.not_before

        queued1 = _util.DelayedJob(Job(10))
        queued2 = _util.DelayedJob(Job(20))
        queued3 = _util.DelayedJob(Job(15))

        assert_true(queued1 < queued2)
        assert_true(queued1 < queued3)
        assert_true(queued3 < queued2)
