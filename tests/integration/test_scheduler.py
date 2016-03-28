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

===========================
 Tests for wolfe.scheduler
===========================

Tests for wolfe.scheduler
"""
if __doc__:  # pragma: no cover
    # pylint: disable = redefined-builtin
    __doc__ = __doc__.encode('ascii').decode('unicode_escape')
__author__ = r"Andr\xe9 Malo".encode('ascii').decode('unicode_escape')
__docformat__ = "restructuredtext en"

import itertools as _it

from nose.tools import assert_equals, assert_true, assert_raises
from .. import _util as _test

import wolfe as _wolfe

# pylint: disable = protected-access
# pylint: disable = no-member


def test_simple():
    """ scheduler: A single job in default group is properly scheduled """
    _wolfe._todo._lock.validate.side_effect = lambda x: []

    exe = _wolfe.Executor('simple')
    todo = _wolfe.TodoDescription('abc').todo()

    wolfe = _wolfe.Main()
    job_id = wolfe.enter_todo(todo)

    assert_equals(wolfe.request_job(exe).id, job_id)
    assert_equals(wolfe.request_job(exe).id, job_id)

    wolfe.finish_job(exe.uid, job_id, _test.Bunch(failed=False))

    assert_true(wolfe.request_job(exe) is None)


@_test.patch(_wolfe.scheduler._job, '_gen_id')
def test_complex():
    """ scheduler: Multiple jobs are ordered and locked properly """
    _wolfe.scheduler._job._gen_id = _it.count(1).next

    success = _test.Bunch(failed=False)

    exe = _wolfe.Executor('complex')
    exe2 = _wolfe.Executor('complex2')
    exe3 = _wolfe.Executor('complex3')

    todo = _wolfe.TodoDescription('abc').todo()
    todo11 = todo.on_success(_wolfe.TodoDescription('def').todo(
        locks=map(_wolfe.Lock, ['lock1', 'lock2'])
    ))
    todo12 = todo.on_success(_wolfe.TodoDescription('ghi').todo(
        locks=[_wolfe.Lock('lock3')]
    ))
    todo12.on_success(_wolfe.TodoDescription('jkl').todo(
        locks=[_wolfe.Lock('lock1')]
    ))
    todo11.on_success(_wolfe.TodoDescription('mno').todo(
        locks=[_wolfe.Lock('lock1')]
    ))

    wolfe = _wolfe.Main()
    job_id = wolfe.enter_todo(todo)

    assert_equals(job_id, 1)
    assert_equals(wolfe.request_job(exe).id, 1)
    assert_true(wolfe.request_job(exe2) is None)

    # === finish 1 ===
    wolfe.finish_job(exe.uid, 1, success)

    assert_equals(wolfe.request_job(exe).id, 2)
    assert_equals(wolfe.request_job(exe2).id, 3)
    assert_true(wolfe.request_job(exe3) is None)

    # === finish 2 ===
    wolfe.finish_job(exe.uid, 2, success)
    assert_equals(wolfe.request_job(exe).id, 4)
    assert_true(wolfe.request_job(exe3) is None)

    # === finish 3 ===
    with assert_raises(_wolfe.InvalidExecutorError):
        wolfe.finish_job(exe.uid, 3, success)
    wolfe.finish_job(exe2.uid, 3, success)
    assert_true(wolfe.request_job(exe2) is None)

    # === finish 4 ===
    wolfe.finish_job(exe.uid, 4, success)
    assert_equals(wolfe.request_job(exe).id, 5)
    assert_true(wolfe.request_job(exe2) is None)

    # === finish 5 ===
    wolfe.finish_job(exe.uid, 5, success)
    assert_true(wolfe.request_job(exe) is None)

    with assert_raises(_wolfe.JobNotFoundError):
        wolfe.finish_job(exe.uid, 6, success)
