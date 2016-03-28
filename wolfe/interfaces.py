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

============
 Interfaces
============

Interfaces
"""
if __doc__:  # pragma: no cover
    # pylint: disable = redefined-builtin
    __doc__ = __doc__.encode('ascii').decode('unicode_escape')
__author__ = r"Andr\xe9 Malo".encode('ascii').decode('unicode_escape')
__docformat__ = "restructuredtext en"


def implements(obj, *interfaces):
    """
    Check if `obj` implements one or more interfaces.

    The check looks for the ``__implements__`` attribute of ``obj``, which
    is expected to be a sequence containing the implemented interfaces.

    :Parameters:
      `obj` : ``type`` or ``object``
        The object to inspect

      `interfaces` : ``tuple``
        Interface classes to check

    :Return: Are all interfaces implemented?
    :Rtype: ``bool``
    """
    try:
        impls = tuple(obj.__implements__)
    except AttributeError:
        return False

    def subclass(sub, sup, _subclass=issubclass):
        """ Type error proof subclass check """
        try:
            return _subclass(sub, sup)
        except TypeError:
            return False

    # O(n**2), better ideas welcome, however, usually the list is pretty
    # small.
    for interface in interfaces:
        for impl in impls:
            if subclass(impl, interface):
                break
        else:
            return False
    return True


class LockInterface(object):  # pragma: nocover
    """
    Interface for lock objects

    :IVariables:
      `name` : ``str``
        Lock name

      `exclusive` : ``bool``
        Does this lock has to be acquired exclusively?
    """


class ExecutorInterface(object):  # pragma: no cover
    """
    Interface for executor objects

    :IVariables:
      `groups` : sequence
        List of job group names, this executor accepts

      `uid` : ``str``
        Unique identifier for this executor
    """

    def attempt(self):
        """
        Create an execution attempt

        :Return: A new attempt container
        :Rtype: `ExecutionAttemptInterface`
        """


class ExecutionAttemptInterface(object):  # pragma: no cover
    """
    Interface for execution attempt containers

    :IVariables:
      `executor` : ``str``
        Executor ID

      `start` : ``float``
        Start time in seconds since epoch

      `end` : ``float``
        End time in seconds since epoch, or ``None``

      `result` : `ExecutionResultInterface`
        Execution result or ``None``
    """

    def finish(self, end, result):
        """
        Mark this attempt as finished by setting end time and result

        :Parameters:
          `end` : ``float``
            End time in seconds since epoch

          `result` : `ExecutionResultInterface`
            Execution result
        """


class ExecutionResultInterface(object):  # pragma: no cover
    """
    Interface for execution result containers

    :IVariables:
      `failed` : ``bool``
        Did the job attempt fail?
    """


class JunkYardInterface(object):  # pragma: no cover
    """ Interface for junk yard (dump for successfully finished jobs) """

    def put(self, job):
        """
        Put a successfully finished job onto the dump

        :Parameters:
          `job` : `JobInterface`
            Job to dump
        """


class JobInterface(object):  # pragma: no cover
    """
    Interface for jobs after they have been finished

    :IVariables:
      `id` : ``int``
        Job ID

      `desc` : `TodoDescription`
        Job description

      `group` : ``str``
        Job Group

      `locks` : ``tuple``
        List of locks that need to be aquired, ordered alphabetically.
        (``(str, ...)``)

      `locks_waiting` : ``int``
        Number of locks to be acquired. ``None`` if
        undetermined yet.

      `importance` : ``int``
        Job importance

      `not_before` : various
        execute job not before this time.

      `extra` : ``dict``
        Extra job data

      `predecessors` : ``set``
        List of jobs to be run successfully before this one (``(int, ...)``)

      `predecessors_waiting` : ``int``
        Number of predecessors this job still has to wait for. ``None`` if
        undetermined yet.

      `attempts` : ``list``
        List of execution attempts
    """

    def depend_on(self, job_id):
        """
        Add predecessor job ID to the job

        :Parameters:
          `job_id` : ``int``
            Job ID to depend on. Must be smaller than self.id and greater than
            0.

        :Exceptions:
          - `ValueError` : job_id was invalid
        """
