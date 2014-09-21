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

================
 Scheduler Main
================

The scheduler is the main dispatcher. It's shared between various objects,
both higher level (the main api) and lower level (where it's referenced with
weakrefs).
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import time as _time

from wolfe import _constants
from wolfe.scheduler import _group
from wolfe.scheduler import _job
from wolfe.scheduler import _job_queue
from wolfe.scheduler import _locks
from wolfe.scheduler import _util
from wolfe.scheduler import _waiting

DependencyCycle = _job.DependencyCycle


class Scheduler(object):
    """
    Container / Dispatcher for all jobs in all states

    :IVariables:
      `jobs` : ``dict``
        Job ID -> job mapping

      `_executing` : ``dict``
        Job ID -> attempt mapping

      `_executors` : ``dict``
        Executor -> Job ID

      `_finished` : `JunkYardInterface`
        Finished job dump

      `_locks` : `Locks`
        Lock manager

      `_delayed` : `JobQueue`
        Queue containing the delayed jobs, ordered by scheduling time

      `_waiting` : `Waiting`
        Jobs waiting for successful other jobs

      `_failed` : ``set``
        Failed job IDs

      `_groups` : ``dict``
        Job group mapping (``{str: Group, ...}``)
    """

    def __init__(self, finished):
        """
        Initialization

        :Parameters:
          `finished` : `JunkYardInterface`
            Finished job dump
        """
        self.jobs = {}
        self._executing = {}
        self._executors = {}
        self._finished = finished
        self._locks = _locks.Locks(self)
        self._delayed = _job_queue.JobQueue(_util.DelayedJob)
        self._waiting = _waiting.Waiting(self)
        self._failed = set()
        self._groups = {}

    def is_done(self, job_id):
        """
        Check if a job ID is successfully done

        :Parameters:
          `job_id` : ``int``
            ID of the job to inspect

        :Return: Is the referenced job done?
        :Rtype: ``bool``
        """
        # Either the job is somewhere around or it's unknown. The latter is
        # defined to be successfully finished, if job_id is not larger than
        # the maximum ID ever given (it cannot be done, if we haven't even
        # seen it yet)
        return 0 < job_id <= _job.last_job_id() and job_id not in self.jobs

    def execution_attempt(self, job_id):
        """
        Find the current execution attempt of a job

        :Parameters:
          `job_id` : ``int``
            Job ID

        :Return: The execution attempt or ``None``, if the job is not executed
                 right now (or if it does not exist at all)
        :Rtype: `ExecutionAttemptInterface`
        """
        return self._executing.get(job_id)

    def get_group(self, name):
        """
        Return job group, create if needed

        :Parameters:
          `name` : ``str``
            Group name

        :Return: The job group instance
        :Rtype: `_group.Group`
        """
        if name not in self._groups:
            self._groups[name] = _group.Group(name, self._locks, self)
        return self._groups[name]

    def del_group(self, name):
        """
        Remove empty group by name

        This is a noop, if the group does not exist. It's asserted that the
        group is empty.

        :Parameters:
          `name` : ``str``
            Group name
        """
        try:
            group = self._groups.pop(name)
            if group:
                self._groups[name] = group
                raise AssertionError("Group is not empty")
        except KeyError:
            pass

    def enter_todo(self, todo):
        """
        Turn todo (graph) into a list of jobs and enter them into the system

        :Parameters:
          `todo` : `Todo`
            The todo to enter into the system

        :Return: The job ID. If the todo was actually a todo graph, the ID of
                 the root job is returned.
        :Rtype: ``int``

        :Exceptions:
          - `DependencyCycle` : The todo graph contains a cycle
        """
        job_id = None
        for job in _job.joblist_from_todo(todo):
            if job_id is None:
                job_id = job.id
            self._enter_job(job)

        return job_id

    def _enter_job(self, job):
        """
        Enter a new job into the system

        "Into the system" means one of the following things:

        - the job is added to ``self.jobs``
        - if the job execution is timed and in the future, it's put into the
          "delayed" queue
        - otherwise, if the job has to wait for other jobs, it's added to the
          "waiting" set
        - otherwise, the job is announced to the lock manager. If the locks
          can't be acquired, the job is stuck and passed to the next step,
          once the locks *can* be acquired
        - otherwise the job is entered into its group queue, where it can be
          picked up by workers

        :Parameters:
          `job` : `JobInterface`
            The job
        """
        self.jobs[job.id] = job
        if job.not_before:
            self._delayed.put(job)
        else:
            self._enter_undelayed(job)

    def _enter_undelayed(self, job):
        """
        Enter job, which is not delayed

        - if the job has to wait for other jobs, it's added to the "waiting"
          set
        - otherwise, the job is announced to the lock manager. if the locks
          can't be acquired, the job is stuck and passed to the next step,
          once the locks *can* be acquired
        - otherwise the job is entered into its group queue, where it can be
          picked up by workers

        :parameters:
          `job` : `JobInterface`
            the job
        """
        if not self._waiting.put(job):
            self._schedule_independent(job)

    def _schedule_independent(self, job):
        """
        Schedule job, which is neither delayed nor depending on another job

        - the job is announced to the lock manager. if the locks can't be
          acquired, the job is stuck and passed to the next step, once the
          locks *can* be acquired
        - otherwise the job is entered into its group queue, where it can be
          picked up by workers

        :Parameters:
          `job` : `JobInterface`
            the job
        """
        self._locks.enter(job)
        self.get_group(job.group).schedule(job)

    def _undelay_jobs(self):
        """
        Schedule jobs, which were delayed until now

        - if no job is delayed until now, this call is a no-op
        - otherwise, if the job has to wait for other jobs, it's added to the
          "waiting" set
        - otherwise, the job is announced to the lock manager. If the locks
          can't be acquired, the job is stuck and passed to the next step,
          once the locks *can* be acquired
        - otherwise the job is entered into its group queue, where it can be
          picked up by workers
        """
        delayed = self._delayed
        if delayed:
            now = int(_time.time())
            while delayed and delayed.peek().not_before <= now:
                self._enter_undelayed(delayed.get())

    def _unwait_jobs(self, finished_id):
        """
        Schedule freed jobs depending on a now finished job

        - the job is announced to the lock manager. if the locks can't be
          acquired, the job is stuck and passed to the next step, once the
          locks *can* be acquired
        - otherwise the job is entered into its group queue, where it can be
          picked up by workers

        :Parameters:
          `finished_id` : ``int``
            ID of the finished job (this property is asserted)
        """
        assert self.is_done(finished_id)

        # need to maintain proper order, because all those jobs are
        # equal regarding the timing, so we basically go by standard queue
        # ordering.
        queue = _job_queue.JobQueue(_util.QueuedJob)
        for job in self._waiting.free(finished_id):
            queue.put(job)
        for job in queue:
            self._schedule_independent(job)

    def request_job(self, executor):
        """
        Find a job for execution

        The job is searched in the executor's groups' queues. The first job
        based on the standard scheduling ordering is marked as being executed
        and returned.

        If the executor does not specifiy any groups, the default group is
        assumed.

        :Parameters:
          `executor` : `ExecutorInterface`
            Executor requesting the job

        :Return: The next job to be executed (and marked as such), or
                 ``None``, if no matching job is scheduled right now.
        :Rtype: `JobInterface`
        """
        if executor.uid in self._executors:
            job_id = self._executors[executor.uid]
            assert job_id in self._executing
            assert job_id in self.jobs
            assert self._executing[job_id].executor == executor.uid
            return self.jobs[job_id]

        self._undelay_jobs()

        found = None
        for group in executor.groups or (_constants.Group.DEFAULT,):
            if group in self._groups:
                group = self._groups[group]
                queued_job = group.peek()
                if queued_job is not None and \
                        (found is None or queued_job < found[0]):
                    found = queued_job, group

        if found is not None:
            assert found[0].job.id not in self._executing

            found = found[1].get()
            self._executing[found.id] = executor.attempt()
            self._executors[executor.uid] = found.id
        return found

    def finish_job(self, job_id, end, result):
        """
        Mark executed job finished

        :Parameters:
          `job_id` : ``int``
            ID of the finished job

          `end` : ``float``
            Finishing time in seconds since epoch

          `result` : `ExecutionResultInterface`
            Execution result
        """
        job = self.jobs[job_id]
        attempt = self._executing.pop(job_id)
        del self._executors[attempt.executor]

        # We need to maintain the proper scheduling order here, because the
        # jobs may re-acquire locks. The queue ensures that order.
        queue = _job_queue.JobQueue(_util.QueuedJob)
        for released in self._locks.release(job):
            queue.put(released)
        for released in queue:
            self.get_group(released.group).schedule(released)

        attempt.finish(end, result)
        job.attempts.append(attempt)

        if not result.failed:  # success
            del self.jobs[job_id]
            self._unwait_jobs(job_id)
            self._finished.put(job)
        else:
            self._fail_job(job)

    def _fail_job(self, job):
        """
        Deal with a failed job

        :Parameters:
          `job` : `JobInterface`
            The failed job
        """
        assert job.id in self.jobs

        self._failed.add(job.id)
