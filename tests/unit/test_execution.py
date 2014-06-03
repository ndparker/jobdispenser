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

============================
 Tests for wolfe._execution
============================

Tests for wolfe._execution.
"""
from __future__ import absolute_import

__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"


from nose.tools import (  # pylint: disable = E0611
    assert_equals, assert_false, assert_true,
)
from .._util import mock, Bunch

from wolfe import _execution

# pylint: disable = E1101


def test_executor_init_1():
    """ Executor initializes uid and no groups """
    result = _execution.Executor('lalala')
    assert_equals(result.uid, 'lalala')
    assert_equals(result.groups, None)


def test_executor_init_2():
    """ Executor initializes uid and empty groups """
    result = _execution.Executor('lololo', [])
    assert_equals(result.uid, 'lololo')
    assert_equals(result.groups, None)


def test_executor_init_3():
    """ Executor initializes uid and single group """
    result = _execution.Executor('lululu', ['g1'])
    assert_equals(result.uid, 'lululu')
    assert_equals(result.groups, ('g1',))


def test_executor_init_4():
    """ Executor initializes uid and multiple groups """
    result = _execution.Executor('foo', ['g1', 'g2'])
    assert_equals(result.uid, 'foo')
    assert_equals(result.groups, ('g1', 'g2'))


@mock(_execution, 'Attempt', name='attempt')
def test_executor_attempt(attempt):
    """ Executor.attempt constructs attempt from self """
    attempt.side_effect = lambda x: Bunch(exe=x)
    exe = _execution.Executor('foo', ['g1', 'g2'])
    result = exe.attempt()

    assert_equals(result.exe, exe)


@mock(_execution, 'Result', name='result')
def test_executor_result(result):
    """ Executor.result constructs result container """
    result.side_effect = lambda *args: Bunch(args=args)
    exe = _execution.Executor('foo', ['g1', 'g2'])
    result = exe.result(1, 2, 3)

    assert_equals(result.args, (1, 2, 3))


@mock(_execution, '_time', name='time')
def test_attempt_init(time):
    """ Attempt initializes properly """
    time.time.side_effect = [42]
    att = _execution.Attempt(Bunch(uid='doh'))

    assert_equals(att.executor, 'doh')
    assert_equals(att.start, 42)
    assert_equals(att.end, None)
    assert_equals(att.result, None)


@mock(_execution, '_time', name='time')
def test_attempt_finish(time):
    """ Attempt.finish stores values """
    time.time.side_effect = [42]
    att = _execution.Attempt(Bunch(uid='doh'))

    assert_equals(att.executor, 'doh')
    assert_equals(att.start, 42)
    assert_equals(att.end, None)
    assert_equals(att.result, None)

    att.finish('END', 'RESULT')
    assert_equals(att.end, 'END')
    assert_equals(att.result, 'RESULT')


def test_result_init():
    """ Result initializes properly """
    result = _execution.Result(0, 'lala', 'lolo')

    assert_equals(result.exit_code, 0)
    assert_false(result.failed)
    assert_equals(result.stdout, 'lala')
    assert_equals(result.stderr, 'lolo')


def test_result_init_2():
    """ Result initializes properly (2) """
    result = _execution.Result(1, 'lalala', 'lololo')

    assert_equals(result.exit_code, 1)
    assert_true(result.failed)
    assert_equals(result.stdout, 'lalala')
    assert_equals(result.stderr, 'lololo')
