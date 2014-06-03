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

==============
 Waiting Jobs
==============

Waiting jobs miss one or more other jobs to be finished successfully.
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import collections as _collections
import weakref as _weakref


class Waiting(object):
    """
    Waiting Job container

    Waiting jobs miss one or more other jobs to be completed successfully.

    This container holds the IDs of all jobs waiting.

    :IVariables:
      `_waiting` : ``set``
        Set of IDs of the jobs waiting

      `_waiting_for` : ``defaultdict(set)``
        Mapping from waiting Job ID to list of IDs of the jobs, the mapped job
        is waiting for

      `_scheduler` : `Scheduler`
        Weak reference to the scheduler
    """

    def __init__(self, scheduler):
        """
        Initialization

        :Parameters:
          `scheduler` : `Scheduler`
            The scheduler instance. It will be stored as a weakref
        """
        self._waiting = set()
        self._waiting_for = _collections.defaultdict(set)
        self._scheduler = _weakref.proxy(scheduler)

    def put(self, job):
        """
        Put a job into waiting state, if needed

        :Parameters:
          `job` : `JobInterface`
            Job to inspect

        :Return: Was it added to waiting?
        :Rtype: ``bool``
        """
        job.predecessors_waiting = len(job.predecessors)
        for job_id in job.predecessors:
            if not self._scheduler.is_done(job_id):
                self._waiting_for[job_id].add(job.id)
            else:
                job.predecessors_waiting -= 1

        assert job.predecessors_waiting >= 0

        if job.predecessors_waiting == 0:
            return False
        self._waiting.add(job.id)
        return True

    def free(self, finished_id):
        """
        Free all jobs waiting for `finished_id`

        :Parameters:
          `finished_id` : ``int``
            ID of the job, which was finished successfully

        :Return: List of IDs of freed jobs, it may be empty.
        :Rtype: ``list``
        """
        assert finished_id not in self._waiting

        freed = []
        for job_id in self._waiting_for.pop(finished_id, ()):
            job = self._scheduler.jobs[job_id]
            job.predecessors_waiting -= 1
            if job.predecessors_waiting == 0:
                freed.append(job)
                self._waiting.remove(job_id)

        return freed
