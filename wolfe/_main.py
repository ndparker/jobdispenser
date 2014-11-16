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
 Main Wolfe API
================

Main wolfe API.
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import time as _time

from ._exceptions import InvalidExecutorError, JobNotFoundError
from . import _junk_yard
from . import scheduler as _scheduler


class Main(object):
    """
    Main API

    :IVariables:
      `_scheduler` : `Scheduler`
        actual job manager
    """

    def __init__(self):
        """ Initialization """
        self._scheduler = _scheduler.Scheduler(_junk_yard.JunkYard())

    def enter_todo(self, todo):
        """
        Enter todo into the system

        :Parameters:
          `todo` : `Todo`
            The todo

        :Return: The assigned job ID. If the passed todo is actually a todo
                 tree, the root job ID is returned
        :Rtype: ``int``
        """
        return self._scheduler.enter_todo(todo)

    def request_job(self, executor):
        """
        Find a job to execute

        :Parameters:
          `executor` : `ExecutorInterface`
            Executor requesting a job
        """
        return self._scheduler.request_job(executor)

    def finish_job(self, ex_id, job_id, result):
        """
        Mark job as finished

        :Parameters:
          `ex_id` : ``str``
            Executor ID

          `job_id` : ``int``
            Job ID

          `result` : `ExecutionResultInterface`
            Execution result
        """
        end = _time.time()
        attempt = self._scheduler.execution_attempt(job_id)
        if attempt is None:
            raise JobNotFoundError(job_id)
        elif ex_id != attempt.executor:
            raise InvalidExecutorError(job_id, ex_id)

        self._scheduler.finish_job(job_id, end, result)
