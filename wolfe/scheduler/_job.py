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

======
 Jobs
======

Jobs have been entered into the scheduler once. They may be even finished
already.
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import collections as _collections
import itertools as _it

from wolfe import _graph
from wolfe import interfaces as _interfaces

#: Exception raised on cycles, when a todo DAG is resolved
DependencyCycle = _graph.DependencyCycle


#: Job ID sequence
#:
#: :Type: callable
_gen_id = _it.count(1).next


def last_job_id():
    """
    Determine the largest job ID assigned until now

    :Return: The ID. It's ``0``, if no job ID was assigned until now (job IDs
             start with ``1``)
    :Rtype: ``id``
    """
    # this inspects the counter iterable by calling pickling methods and
    # retrieving the next value from there and then subtracting one.

    # __reduce__ returns the factory ('count') and the argument tuple
    # containing the initial value (advanced with each call to next())
    return _gen_id.__self__.__reduce__()[1][0] - 1


class Job(object):
    """
    Job after is been scheduled.

    :See: `JobInterface`
    """
    __implements__ = [_interfaces.JobInterface]

    def __init__(self, job_id, desc, group, locks, importance, not_before,
                 extra, predecessors, attempts):
        """
        Initialization

        :Parameters:
          `job_id` : ``int``
            Job ID

          `desc` : `TodoDescription`
            Job description

          `group` : ``str``
            Job Group

          `locks` : iterable
            List of locks that need to be aquired (``(str, ...)``)

          `importance` : ``int``
            Job importance

          `not_before` : various
            execute job not before this time. Special formats are allowed:

            ``int``
              Number of seconds from now (delay)

            ``datetime.datetime``
              a specific point in time (server time). Use UTC if you can. For
              naive date times, UTC is assumed.

            If omitted or ``None``, ``0`` is assumed.

          `extra` : ``dict``
            Extra job data

          `predecessors` : iterable
            List of jobs to be run successfully before this one
            (``(int, ...)``)

          `attempts` : ``list``
            execution attempts (``[ExecutionAttemptInterface, ...]``)
        """
        self.id = job_id  # pylint: disable = C0103
        self.desc = desc
        self.group = group
        self.locks = tuple(sorted(set(locks or ())))
        self.locks_waiting = None
        self.importance = importance
        self.extra = extra
        self.predecessors = set()
        self.predecessors_waiting = None
        self.attempts = attempts
        self.not_before = not_before
        for item in predecessors or ():
            self.depend_on(item)

    def depend_on(self, job_id):
        """
        Add predecessor job ID

        Duplicates are silently ignored.

        :See: `interfaces.JobInterface.depend_on`
        """
        assert self.predecessors_waiting is None

        try:
            job_id = int(job_id)
        except TypeError:
            raise ValueError("Invalid job_id: %r" % (job_id,))
        if job_id < 1 or job_id >= self.id:
            raise ValueError("Invalid job_id: %r" % (job_id,))
        self.predecessors.add(job_id)


def job_from_todo(todo):
    """
    Construct Job from Todo

    :Parameters:
      `todo` : `Todo`
        Todo to construct from

    :Return: New job instance
    :Rtype: `JobInterface`
    """
    return Job(
        _gen_id(), todo.desc, todo.group, todo.locks, todo.importance,
        todo.not_before, {}, set(), []
    )


def joblist_from_todo(todo):
    """
    Construct a list of jobs from Todo graph

    :Parameters:
      `todo` : `Todo`
        todo to be inspected.

    :Return: List of jobs (``[JobInterface, ...]``)
    :Rtype: ``list``
    """
    jobs, todos, virtuals = [], {}, {}
    toinspect = _collections.deque([(todo, None)])
    graph = _graph.DependencyGraph()

    # 1) fill the dependency graph with the todo nodes (detects cycles, too)
    try:
        while toinspect:
            todo, parent = toinspect.pop()
            todo_id = id(todo)
            if todo_id in todos:
                virtual_id, pre, _ = todos[todo_id]
            else:
                pre = []
                virtual_id = len(virtuals)
                todos[todo_id] = virtual_id, pre, todo
                virtuals[virtual_id] = todo_id

                for parent_id in todo.predecessors():
                    graph.add((False, parent_id), (True, virtual_id))
                    pre.append((False, parent_id))

                for succ in todo.successors():
                    toinspect.appendleft((succ, (True, virtual_id)))

            if parent is not None:
                graph.add(parent, (True, virtual_id))
                pre.append(parent)
            else:
                graph.add((False, None), (True, virtual_id))
    except DependencyCycle, e:
        # remap to our input (todos and not some weird virtual IDs)
        raise DependencyCycle([
            todos[virtuals[tup[1]]][2] for tup in e.args[0]
        ])

    # 2) resolve the graph (create topological order)
    id_mapping = {}
    for is_virtual, virtual_id in graph.resolve():
        if is_virtual:
            _, pres, todo = todos[virtuals[virtual_id]]
            job = job_from_todo(todo)
            for is_virtual, pre in pres:
                if is_virtual:
                    pre = id_mapping[pre]
                job.depend_on(pre)

            id_mapping[virtual_id] = job.id
            jobs.append(job)

    return jobs
