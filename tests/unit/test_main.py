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

=======================
 Tests for wolfe._main
=======================

Tests for wolfe._main.
"""
if __doc__:  # pragma: no cover
    # pylint: disable = redefined-builtin
    __doc__ = __doc__.encode('ascii').decode('unicode_escape')
__author__ = r"Andr\xe9 Malo".encode('ascii').decode('unicode_escape')
__docformat__ = "restructuredtext en"

from nose.tools import assert_equals, assert_raises
from .. import _util as _test

from wolfe import _main

# pylint: disable = protected-access


@_test.patch(_main, '_junk_yard', name='junk_yard')
@_test.patch(_main, '_scheduler', name='scheduler')
def test_init(junk_yard, scheduler):
    """ Main properly initializes the scheduler """
    junk_yard.JunkYard = lambda: 'blah'
    scheduler.Scheduler = lambda x: 'blub(%r)' % x

    result = _main.Main()._scheduler

    assert_equals(result, "blub('blah')")


@_test.patch(_main, '_junk_yard')
@_test.patch(_main, '_scheduler')
def test_enter_todo():
    """ Main passes todos to the scheduler """
    main = _main.Main()
    main._scheduler.enter_todo.side_effect = ['lala']

    result = main.enter_todo('zonk')

    assert_equals(result, 'lala')

    # pylint: disable = no-member
    assert_equals(map(tuple, main._scheduler.enter_todo.mock_calls), [
        ('', ('zonk',), {})
    ])


@_test.patch(_main, '_junk_yard')
@_test.patch(_main, '_scheduler')
def test_request_job():
    """ Main requests jobs from the scheduler """
    main = _main.Main()

    main._scheduler.request_job.side_effect = lambda x: 'R(%r)' % (x,)
    result = main.request_job('HUH')

    assert_equals(result, "R('HUH')")


@_test.patch(_main, '_junk_yard')
@_test.patch(_main, '_scheduler')
@_test.patch(_main, '_time', name='time')
def test_finish_job(time):
    """ Main.finish_job passes jobs to the scheduler """
    main = _main.Main()

    time.time.side_effect = [123]
    main._scheduler.execution_attempt.side_effect = \
        {23: _test.Bunch(executor='ex1')}.get
    main.finish_job('ex1', 23, 'result')

    # pylint: disable = no-member
    assert_equals(map(tuple, main._scheduler.finish_job.mock_calls), [
        ('', (23, 123, 'result'), {}),
    ])


@_test.patch(_main, '_junk_yard')
@_test.patch(_main, '_scheduler')
@_test.patch(_main, '_time', name='time')
def test_finish_job_1(time):
    """ Main.finish_job checks executor """
    main = _main.Main()

    time.time.side_effect = [123]
    main._scheduler.execution_attempt.side_effect = \
        {23: _test.Bunch(executor='ex2')}.get

    with assert_raises(_main.InvalidExecutorError) as e:
        main.finish_job('ex1', 23, 'result')
    assert_equals(e.exception.args, (23, 'ex1'))


@_test.patch(_main, '_junk_yard')
@_test.patch(_main, '_scheduler')
@_test.patch(_main, '_time', name='time')
def test_finish_job_2(time):
    """ Main.finish_job checks if job exists """
    main = _main.Main()

    time.time.side_effect = [123]
    main._scheduler.execution_attempt.side_effect = \
        {24: _test.Bunch(executor='ex2')}.get

    with assert_raises(_main.JobNotFoundError) as e:
        main.finish_job('ex1', 23, 'result')
    assert_equals(e.exception.args, (23,))
