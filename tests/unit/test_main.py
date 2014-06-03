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

=======================
 Tests for wolfe._main
=======================

Tests for wolfe._main.
"""
from __future__ import absolute_import

__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"


from nose.tools import (  # pylint: disable = E0611
    assert_equals, assert_false,
)
from .._util import mock, Bunch

from wolfe import _main

# pylint: disable = W0212, W0613, W0108


@mock(_main, '_junk_yard', name='junk_yard')
@mock(_main, '_scheduler', name='scheduler')
def test_init(junk_yard, scheduler):
    """ Main properly initializes the scheduler """
    junk_yard.JunkYard = lambda: 'blah'
    scheduler.Scheduler = lambda x: 'blub(%r)' % x

    result = _main.Main()._scheduler

    assert_equals(result, "blub('blah')")


@mock(_main, '_junk_yard')
@mock(_main, '_scheduler')
def test_enter_todo():
    """ Main passes todos to the scheduler """
    main = _main.Main()
    main._scheduler.enter_todo.side_effect = ['lala']

    result = main.enter_todo('zonk')

    assert_equals(result, 'lala')
    assert_equals(map(tuple, main._scheduler.enter_todo.mock_calls), [
        ('', ('zonk',), {})
    ])


@mock(_main, '_junk_yard')
@mock(_main, '_scheduler')
def test_request_job():
    """ Main requests jobs from the scheduler """
    main = _main.Main()

    main._scheduler.request_job.side_effect = lambda x: 'R(%r)' % (x,)
    result = main.request_job('HUH')

    assert_equals(result, "R('HUH')")


@mock(_main, '_junk_yard')
@mock(_main, '_scheduler')
@mock(_main, '_time', name='time')
def test_finish_job(time):
    """ Main.finish_job passes jobs to the scheduler """
    main = _main.Main()

    time.time.side_effect = [123]
    main._scheduler.execution_attempt.side_effect = \
        lambda x: {23: Bunch(executor='ex1')}.get(x)
    main.finish_job('ex1', 23, 'result')

    assert_equals(map(tuple, main._scheduler.finish_job.mock_calls), [
        ('', (23, 123, 'result'), {}),
    ])


@mock(_main, '_junk_yard')
@mock(_main, '_scheduler')
@mock(_main, '_time', name='time')
def test_finish_job_1(time):
    """ Main.finish_job checks executor """
    main = _main.Main()

    time.time.side_effect = [123]
    main._scheduler.execution_attempt.side_effect = \
        lambda x: {23: Bunch(executor='ex2')}.get(x)
    try:
        main.finish_job('ex1', 23, 'result')
    except _main.InvalidExecutorError, e:
        assert_equals(e.args, (23, 'ex1'))
    else:
        assert_false("Exception is not raised")


@mock(_main, '_junk_yard')
@mock(_main, '_scheduler')
@mock(_main, '_time', name='time')
def test_finish_job_2(time):
    """ Main.finish_job checks if job exists """
    main = _main.Main()

    time.time.side_effect = [123]
    main._scheduler.execution_attempt.side_effect = \
        lambda x: {24: Bunch(executor='ex2')}.get(x)
    try:
        main.finish_job('ex1', 23, 'result')
    except _main.JobNotFoundError, e:
        assert_equals(e.args, (23,))
    else:
        assert_false("Exception is not raised")
