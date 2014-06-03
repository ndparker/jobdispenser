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
 Job execution objects
=======================

Job execution objects.
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import time as _time

from wolfe import interfaces as _interfaces


class Executor(object):
    """
    Executor info container

    :See: `ExecutorInterface`
    """
    __implements__ = [_interfaces.ExecutorInterface]

    def __init__(self, uid, groups=None):
        """
        Initialization

        :Parameters:
          `uid` : ``str``
            Unique identifier for this executor

          `groups` : iterable
            List of groups to be executed by this executor. If omitted or
            ``None``, the default group will be used
        """
        self.groups = tuple(groups or ()) or None
        self.uid = uid

    def attempt(self):
        """
        Create an execution attempt

        :See: `interfaces.ExecutorInterface.attempt`
        """
        return Attempt(self)

    def result(self, exit_code, stdout, stderr):
        """
        Create an execution result

        :Parameters:
          `exit_code` : ``int``
            Exit code of the job process. Zero means success.

          `stdout` : ``str``
            Stdout of the job process

          `stderr` : ``str``
            Stderr of the job process

        :Return: The result container
        :Rtype: `ExecutionResultInterface`
        """
        return Result(exit_code, stdout, stderr)


class Attempt(object):
    """
    Execution attempt

    :See: `ExecutionAttemptInterface`
    """
    __implements__ = [_interfaces.ExecutionAttemptInterface]

    def __init__(self, executor):
        """
        Initialization

        :Parameters:
          `executor` : `ExecutorInterface`
            Executor instance
        """
        self.executor = executor.uid
        self.start = _time.time()
        self.end = None
        self.result = None

    def finish(self, end, result):
        """
        Mark this attempt as finished

        :See: `interfaces.ExecutionAttemptInterface.finish`
        """
        self.end = end
        self.result = result


class Result(object):
    """
    Execution result

    :See: `ExecutionResultInterface`
    """
    __implements__ = [_interfaces.ExecutionResultInterface]

    def __init__(self, exit_code, stdout, stderr):
        """
        Initialization

        :Parameters:
          `exit_code` : ``int``
            Exit code of the job process. Zero means success.

          `stdout` : ``str``
            Stdout of the job process

          `stderr` : ``str``
            Stderr of the job process
        """
        self.exit_code = exit_code
        self.failed = exit_code != 0
        self.stdout = stdout
        self.stderr = stderr
