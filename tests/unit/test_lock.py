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
 Tests for wolfe._lock
=======================

Tests for wolfe._lock.
"""
from __future__ import absolute_import

__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"


from nose.tools import (  # pylint: disable = E0611
    assert_equals, assert_raises
)
from .._util import Bunch

from wolfe import _lock

# pylint: disable = C0103


def test_lock_init_minimal():
    """ Lock properly initializes minimalistically"""
    lock = _lock.Lock("somename")

    assert_equals(lock.name, "somename")
    assert_equals(lock.exclusive, True)


def test_lock_init_maximal():
    """ Lock properly initializes will full arguments """
    lock = _lock.Lock("someothername", exclusive=True)
    lock2 = _lock.Lock("someothername2", exclusive=False)

    assert_equals(lock.name, "someothername")
    assert_equals(lock.exclusive, True)
    assert_equals(lock2.name, "someothername2")
    assert_equals(lock2.exclusive, False)


def test_lock_validate_none():
    """ _lock.validate returns empty list for None """
    assert_equals([], _lock.validate(None))


def test_lock_validate_empty():
    """ _lock.validate returns empty list for empty sequence """
    assert_equals([], _lock.validate(iter(())))


def test_lock_validate_happy():
    """ _lock.validate returns ordered, squashed list of locks """
    lock1 = Bunch(name='foo', exclusive=True)
    lock2 = Bunch(name='baz', exclusive=False)
    lock3 = Bunch(name='foo', exclusive=True)
    lock4 = Bunch(name='bar', exclusive=True)

    result = _lock.validate([lock1, lock2, lock3, lock4])
    assert_equals(result, [lock4, lock2, lock1])


def test_lock_validate_conflict():
    """ _lock.validate raise exception on conflicting locks """
    lock1 = Bunch(name='foo', exclusive=True)
    lock2 = Bunch(name='baz', exclusive=False)
    lock3 = Bunch(name='foo', exclusive=False)
    lock4 = Bunch(name='bar', exclusive=True)

    with assert_raises(_lock.LockConflict):
        _lock.validate([lock1, lock2, lock3, lock4])
