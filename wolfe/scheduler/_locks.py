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

=====================
 Job Lock Management
=====================

Job locks are acquired globally. They are managed by the `Locks` class.
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import collections as _collections
import weakref as _weakref


class Locks(object):
    """
    Lock manager

    :IVariables:
      `_waiting` : ``dict``
        Mapping of locks to job IDs, waiting for release (acquired by another
        job)

      `_free` : ``dict``
        Mapping of locks to job IDs, free to acquire

      `_acquired` : ``dict``
        Mapping of currently acquired locks to job IDs

      `_scheduler` : `Scheduler`
        Scheduler instance (weakref)
    """

    def __init__(self, scheduler):
        """
        Initialization

        :Parameters:
          `scheduler` : `Scheduler`
            Scheduler instance, this lock manager is bound to
        """
        self._waiting = _collections.defaultdict(set)
        self._free = _collections.defaultdict(set)
        self._acquired = {}
        self._scheduler = _weakref.proxy(scheduler)

    def enter(self, job):
        """
        Enter locks of a job to the system

        :Parameters:
          `job` : `JobInterface`
            Job whose locks should be entered
        """
        job.locks_waiting = len(job.locks)
        for lock in job.locks:
            assert lock.exclusive

            if lock.name in self._acquired:
                self._waiting[lock.name].add(job.id)
            else:
                self._free[lock.name].add(job.id)
                job.locks_waiting -= 1

        assert job.locks_waiting >= 0

    def acquire(self, job):
        """
        Acquire locks for job

        :Parameters:
          `job` : `JobInterface`
            Job to acquire the locks for

        :Return: Locks acquired?
        :Rtype: ``bool``
        """
        if job.locks_waiting:
            return False

        jobs = self._scheduler.jobs
        for lock in job.locks:
            assert lock.name not in self._acquired

            waiting = self._free.pop(lock.name)
            waiting.remove(job.id)
            if waiting:
                self._waiting[lock.name] = waiting
                for job_id in waiting:
                    jobs[job_id].locks_waiting += 1
            self._acquired[lock.name] = job.id

        return True

    def release(self, job):
        """
        Release locks for a job

        :Parameters:
          `job` : `JobInterface`
            Job to release the locks for

        :Return: List of jobs, which have free locks now
        :Rtype: ``list``
        """
        assert job.locks_waiting == 0

        candidates = set()

        jobs = self._scheduler.jobs
        for lock in job.locks:
            assert self._acquired[lock.name] == job.id

            del self._acquired[lock.name]
            if lock.name in self._waiting:
                free = self._free[lock.name] = self._waiting.pop(lock.name)
                for job_id in free:
                    jobs[job_id].locks_waiting -= 1
                    if jobs[job_id].locks_waiting == 0:
                        candidates.add(job_id)

        return map(lambda x: jobs[x], candidates)  # pylint: disable = W0110
