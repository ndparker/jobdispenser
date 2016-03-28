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

=====================
 Scheduler Utilities
=====================

Collection of scheduler-internal utilities.
"""
if __doc__:  # pragma: no cover
    # pylint: disable = redefined-builtin
    __doc__ = __doc__.encode('ascii').decode('unicode_escape')
__author__ = r"Andr\xe9 Malo".encode('ascii').decode('unicode_escape')
__docformat__ = "restructuredtext en"

import datetime as _dt
import time as _time

try:  # pragma: no cover
    import pytz as _pytz
except ImportError:  # pragma: no cover
    _pytz = None


class DelayedJob(object):
    """
    Ordering wrapper for job inside the scheduled queue

    :IVariables:
      `job` : any
        The wrapped job
    """

    def __init__(self, job):
        """
        Initilization

        :Parameters:
          `job` : any
            The job to wrap
        """
        self.job = job
        self.not_before = scheduled_time(job)

    def __lt__(self, other):
        """
        Compare jobs by schedule time

        :Parameters:
          `other` : `DelayedJob`
            The job to compare ourself to

        :Return: Is this job "smaller" than the other?
        :Rtype: ``bool``
        """
        return self.not_before < other.not_before


class QueuedJob(object):
    """
    Ordering wrapper for job inside the main queue

    :IVariables:
      `job` : any
        The wrapped job
    """

    def __init__(self, job):
        """
        Initilization

        :Parameters:
          `job` : any
            The job to wrap
        """
        self.job = job

    def __lt__(self, other):
        """
        Compare jobs by importance and then ID

        :Parameters:
          `other` : `QueuedJob`
            The job to compare ourself to

        :Return: Is this job "smaller" than the othe?
        :Rtype: ``bool``
        """
        return (
            self.job.importance > other.job.importance
            or
            self.job.id < other.job.id
        )


def scheduled_time(job):
    """
    Determined the scheduled time for a job

    :Parameters:
      `job` : `JobInterface`
        Job to inspect

    :Return: Scheduled time of the job in seconds since epoch
    :Rtype: ``int``
    """
    time_now = int(_time.time())
    not_before = job.not_before
    if not not_before:
        return time_now

    try:
        not_before = int(not_before)
    except (TypeError, ValueError):
        now = _dt.datetime.utcnow()
        if not_before.tzinfo is not None:
            if _pytz is None:
                raise RuntimeError("Need pytz for timezone support")
            # pylint: disable = no-value-for-parameter
            now = _pytz.UTC.localize(now)
        not_before = max(0, int((not_before - now).total_seconds()))

    return time_now + not_before
