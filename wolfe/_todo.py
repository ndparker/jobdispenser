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

=======
 Todos
=======

Todo containers.
"""
if __doc__:  # pragma: no cover
    # pylint: disable = redefined-builtin
    __doc__ = __doc__.encode('ascii').decode('unicode_escape')
__author__ = r"Andr\xe9 Malo".encode('ascii').decode('unicode_escape')
__docformat__ = "restructuredtext en"

import datetime as _dt

from . import _constants
from . import _lock


class Todo(object):
    """
    An actual todo based on a todo description

    :IVariables:
      `desc` : `TodoDescription`
        Todo descripton

      `locks` : ``list``
        List of locks

      `importance` : ``int``
        Importance

      `group` : ``str``
        Job group

      `not_before` : various
        execute job not before this time

      `_successors` : ``list``
        List of jobs or todos depending on successful execution of this one
        (``[JobOrTodo, ...]``)

      `_predecessors` : ``list``
        List of job IDs this job depends on (``[int, ...]``)
    """

    def __init__(self, desc, depends_on=None, locks=None, importance=None,
                 group=None, not_before=None):
        """
        Initialization

        :Parameters:
          `desc` : `TodoDescription`
            Todo description

          `depends_on` : iterable
            List of todos or jobs this todo depends on. If omitted or
            ``None``, this todo is constructed without dependencies.
            (``[Todo_or_ID, ...]``)

          `locks` : iterable
            List of locks. If omitted or ``None``, no locks are acquired for
            execution. (``[LockInterface, ...]``)

          `importance` : ``int``
            Importance of the todo. If omitted or ``None``, a default
            importance is assigned.

          `group` : ``str``
            Job group. If omitted or ``None``, a default group is assigned.

          `not_before` : various
            execute job not before this time. Special formats are allowed:

            ``int``
              Number of seconds from now (delay)

            ``datetime.datetime``
              a specific point in time (server time). Use UTC if you can. For
              naive date times, UTC is assumed.

            If omitted or ``None``, ``0`` is assumed.

        :Exceptions:
          - `LockConflict` : Conflicting locks were provided
        """
        self.desc = desc
        self._successors = []
        self._predecessors = []
        self.locks = _lock.validate(locks)
        if importance is None:
            importance = _constants.Importance.DEFAULT
        self.importance = importance
        if group is None:
            group = _constants.Group.DEFAULT
        self.group = group

        if not_before:
            try:
                not_before = max(0, int(not_before))
            except (TypeError, ValueError):
                pass
            else:
                if not_before:
                    not_before = (
                        _dt.datetime.utcnow()
                        + _dt.timedelta(seconds=not_before)
                    )
        self.not_before = not_before

        if depends_on is not None:
            for todo in depends_on:
                try:
                    on_success = todo.on_success
                except AttributeError:
                    job = int(todo)
                    assert job > 0
                    self._predecessors.append(job)
                else:
                    on_success(self)

    def on_success(self, todo):
        """
        Add a todo as post-dependency

        :Parameters:
          `todo` : `Todo`
            Todo to chain after self.

        :Return: The added todo again (for easier chaining)
        :Rtype: `Todo`
        """
        self._successors.append(todo)
        return todo

    def predecessors(self):
        """
        Return todo's predecessors

        :Return: List of job IDs (``(int, ...)``)
        :Rtype: ``tuple``
        """
        return tuple(self._predecessors)

    def successors(self):
        """
        Return todo's successors

        :Return: List of todos (``(Todo, ...)``)
        :Rtype: ``tuple``
        """
        return tuple(self._successors)


class TodoDescription(object):
    """
    Todo description

    :IVariables:
      `name` : ``str``
        Descriptive name

      `locks` : ``tuple``
        Default locks or ``None``

      `importance` : ``int``
        Default importance or ``None``

      `group` : ``str``
        Default group name or ``None``
    """

    def __init__(self, name, locks=None, importance=None, group=None):
        """
        Initialization

        :Parameters:
          `name` : ``str``
            Descriptive name

          `locks` : iterable
            Default locks. If omitted or ``None``, no default locks are
            defined.

          `importance` : ``int``
            Default importance. If omitted or ``None``, no default importance
            is defined.

          `group` : ``str``
            Default group name. If omitted or ``None``, no default group is
            defined.

        :Exceptions:
          - `LockConflict` : Conflicting locks were provided
        """
        self.name = name
        self.locks = _lock.validate(locks) or None
        self.importance = importance
        self.group = group

    def todo(self, depends_on=None, locks=None, importance=None, group=None,
             not_before=None):
        """
        Construct a todo from this description

        :Parameters:
          `depends_on` : iterable
            List of jobs or todos this todo should depend on (``(Todo_or_id,
            ...)``)

          `locks` : iterable
            Locks. If omitted or ``None``, the default locks are applied.

          `importance` : ``int``
            Default importance. If omitted or ``None``, the default importance
            is applied.

          `group` : ``str``
            Default group name. If omitted or ``None``, the default group is
            applied.

          `not_before` : various
            execute job not before this time. Special formats are allowed:

            ``None`` or ``'now'``
              Execute as soon as possible

            ``int``
              Number of seconds from now (delay)

            ``datetime.datetime``
              a specific point in time

        :Return: new todo instance
        :Rtype: `Todo`
        """
        if locks is None:
            locks = self.locks
        if importance is None:
            importance = self.importance
        if group is None:
            group = self.group
        return Todo(
            self,
            depends_on=depends_on,
            locks=locks,
            importance=importance,
            group=group,
            not_before=not_before,
        )
