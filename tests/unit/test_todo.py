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
 Tests for wolfe._todo
=======================

Tests for wolfe._todo.
"""
from __future__ import absolute_import

__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"


from nose.tools import (  # pylint: disable = E0611
    assert_equals,
)
from .._util import mock, Bunch

from wolfe import _todo

# pylint: disable = C0103, W0212


@mock(_todo, '_constants', name='constants')
def test_todo_init_minimal(constants):
    """ Todo properly initializes minimalistically"""
    constants.Group.DEFAULT = 'some group'
    constants.Importance.DEFAULT = 23

    todo = _todo.Todo("DESC")

    assert_equals(todo.__dict__, {
        '_predecessors': [],
        '_successors': [],
        'desc': 'DESC',
        'group': 'some group',
        'importance': 23,
        'locks': set([]),
        'not_before': None,
    })


@mock(_todo, '_constants', name='constants')
def test_todo_init_maximal(constants):
    """ Todo properly initializes will full arguments """
    constants.Group.DEFAULT = 'some group xx'
    constants.Importance.DEFAULT = 24

    other = []
    not_before = object()

    todo = _todo.Todo(
        "DESCX",
        depends_on=iter([1, 2, Bunch(on_success=other.append), 3]),
        locks=['krass', 'krasser'],
        importance=2,
        group='another group',
        not_before=not_before,
    )

    assert_equals(todo.__dict__, {
        '_predecessors': [1, 2, 3],
        '_successors': [],
        'desc': 'DESCX',
        'group': 'another group',
        'importance': 2,
        'locks': set(['krass', 'krasser']),
        'not_before': not_before,
    })
    assert_equals(other, [todo])


@mock(_todo, '_dt', name='dt')
@mock(_todo, '_constants', name='constants')
def test_todo_init_int_delay(dt, constants):
    """ Todo properly initializes will integer delay """
    dt.datetime.utcnow.side_effect = [12]
    dt.timedelta.side_effect = lambda seconds=None: seconds
    constants.Group.DEFAULT = 'some group xx'
    constants.Importance.DEFAULT = 24

    other = []

    todo = _todo.Todo(
        "DESCX",
        depends_on=iter([1, 2, Bunch(on_success=other.append), 3]),
        locks=['krass', 'krasser'],
        importance=2,
        group='another group',
        not_before=17,
    )

    assert_equals(todo.__dict__, {
        '_predecessors': [1, 2, 3],
        '_successors': [],
        'desc': 'DESCX',
        'group': 'another group',
        'importance': 2,
        'locks': set(['krass', 'krasser']),
        'not_before': 29,
    })
    assert_equals(other, [todo])


@mock(_todo, '_constants', name='constants')
def test_todo_on_success(constants):
    """ Todo stores and returns successors properly """
    constants.Group.DEFAULT = 'some group xx'
    constants.Importance.DEFAULT = 24

    todo = _todo.Todo("ASdF")
    successor1 = object()
    successor2 = object()

    assert successor1 is not successor2

    result1 = todo.on_success(successor2)
    result2 = todo.on_success(successor1)

    assert successor2 is result1
    assert successor1 is result2

    assert_equals(todo._successors, [successor2, successor1])
    assert_equals(todo.successors(), (successor2, successor1))


@mock(_todo, '_constants', name='constants')
def test_todo_predecessors(constants):
    """ Todo returns predecessors properly """
    constants.Group.DEFAULT = 'some group xx'
    constants.Importance.DEFAULT = 24

    other = []

    todo = _todo.Todo(
        "DESCX",
        depends_on=iter([1, 2, Bunch(on_success=other.append), 3]),
    )

    assert_equals(todo.predecessors(), (1, 2, 3))


def test_desc_init_minimal():
    """ TodoDescription properly initializes minimalistically """
    desc = _todo.TodoDescription("DESC")

    assert_equals(desc.__dict__, {
        'group': None, 'importance': None, 'locks': None, 'name': 'DESC'
    })


def test_desc_init_maximal():
    """ TodoDescription properly initializes with full arguments """
    desc = _todo.TodoDescription("DESC", locks=[4, 8], importance=5, group=6)

    assert_equals(desc.__dict__, {
        'group': 6, 'importance': 5, 'locks': (4, 8), 'name': 'DESC'
    })


@mock(_todo, 'Todo', name='todo_mock')
def test_desc_todo(todo_mock):
    """ TodoDescription.todo picks default values """
    todo_mock.side_effect = ['lala']
    desc = _todo.TodoDescription("DESC", locks=[4, 8], importance=5, group=6)

    todo = desc.todo()

    assert_equals(todo, 'lala')
    assert_equals(map(tuple, todo_mock.mock_calls), [(
        '',
        (desc,),
        {
            'depends_on': None,
            'group': 6,
            'importance': 5,
            'locks': (4, 8),
            'not_before': None
        }
    )])


@mock(_todo, 'Todo', name='todo_mock')
def test_desc_todo_override_defaults(todo_mock):
    """ TodoDescription.todo overrides default values """
    todo_mock.side_effect = ['lala']
    desc = _todo.TodoDescription("DESC", locks=[4, 8], importance=5, group=6)

    todo = desc.todo(
        depends_on=18, locks=[20, 22], importance=23, group=24, not_before=28
    )

    assert_equals(todo, 'lala')
    assert_equals(map(tuple, todo_mock.mock_calls), [(
        '',
        (desc,),
        {
            'depends_on': 18,
            'group': 24,
            'importance': 23,
            'locks': [20, 22],
            'not_before': 28
        }
    )])