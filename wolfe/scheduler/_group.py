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

===========
 Job Group
===========

Job Groups contain the queues for ready-to-do-jobs and are asked by the API
for the next job.
"""
if __doc__:  # pragma: no cover
    # pylint: disable = redefined-builtin
    __doc__ = __doc__.encode('ascii').decode('unicode_escape')
__author__ = r"Andr\xe9 Malo".encode('ascii').decode('unicode_escape')
__docformat__ = "restructuredtext en"

import weakref as _weakref

from . import _job_queue
from . import _util


class Group(object):
    """
    Job Group

    :IVariables:
      `name` : ``str``
        Group name

      `_locks` : `Locks`
        Lock manager

      `_scheduler` : `Scheduler`
        Scheduler (weakly referenced)

      `_queue` : `JobQueue`
        Actual queue
    """

    def __init__(self, name, locks, scheduler):
        """
        Initialization

        :Parameters:
          `name` : ``str``
            Group name

          `locks` : `Locks`
            Lock manager

          `scheduler` : `Scheduler`
            Scheduler
        """
        self.name = name
        self._locks = locks
        self._scheduler = _weakref.proxy(scheduler)
        self._queue = _job_queue.JobQueue(_util.QueuedJob)

    def __nonzero__(self):
        """
        Return false if the queue is empty, true otherwise

        :Return: Is there something in the queue?
        :Rtype: ``bool``
        """
        return bool(self._queue)

    def schedule(self, job):
        """
        Put in a new job, if feasible

        If the job has any locks attached, the method tries to acquire them.
        If this was successful, the job is entered into the queue. Otherwise
        nothing happens.

        :Parameters:
          `job` : `JobInterface`
            The job to put in.

        :Return: Was it added?
        :Rtype: ``bool``
        """
        if job.locks_waiting:
            return False

        acquired = self._locks.acquire(job)
        if not acquired:
            raise AssertionError("Lock inconsistency. Should not happen (TM)")
        self._queue.put(job)
        return True

    def peek(self):
        """
        Peek at the next job

        :Return: The next job todo for this group, or ``None``, if there"s
                 nothing to do right now
        :Rtype: `QueuedJob`
        """
        if not self._queue:
            return None
        return self._queue.peek()

    def get(self):
        """
        Pick the next job

        The job is removed from the queue. If the queue is empty afterwards,
        the group reference is removed from the scheduler.

        :Return: The job
        :Rtype: `JobInterface`

        :Exceptions:
          - `IndexError` : The queue was empty
        """
        try:
            return self._queue.get()
        finally:
            if not self._queue:
                self._scheduler.del_group(self.name)
