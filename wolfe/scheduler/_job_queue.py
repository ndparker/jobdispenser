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

===========
 Job Queue
===========

Job Queue. The queue is implemented as priority queue using a heap.
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import heapq as _heapq


class JobQueue(object):
    """
    Job queue

    This container utilizes a heap structure to implement a more or less
    generic priority queue (see below). The sorting order of the items is
    defined by a wrapper class passed to the constructor.

    The queue is made for jobs. That's why wrapper classes have to provide a
    job attribute for unwrapping and items passed into the queue are expected
    to provide a valid ``id`` attribute.

    Additionally the queue implements boolean operations (it's false if it's
    empty) and a __contains__ operation based on job IDs.

    >>> class Wrapper(object):
    ...     def __init__(self, job):
    ...         self.job = job
    ...     def __lt__(self, other):
    ...         return self.job.id > other.job.id
    >>> class Job(object):
    ...     def __init__(self, job_id):
    ...         self.id = job_id
    >>> queue = JobQueue(Wrapper)

    >>> queue.put(Job(2))
    >>> bool(queue)
    True
    >>> 1 in queue
    False
    >>> 2 in queue
    True
    >>> len(queue)
    1

    :IVariables:
      `_queue` : ``list``
        actual heap containing wrapped jobs

      `_wrapper` : callable
        Wrapper class factory

      `_ids` : ``set``
        Set of job IDs currently queued
    """

    def __init__(self, wrapper_class):
        """
        Initialization

        :Parameters:
          `wrapper_class` : any
            class factory expected to take a job and represent it inside the
            queue. The object should be comparable with other instances
            (``__lt__`` is the proper method) and should provide a ``job``
            attribute pointing to the original object.
        """
        self._queue = []
        self._wrapper = wrapper_class
        self._ids = set()

    def __nonzero__(self):
        """
        Return false if the queue is empty, true otherwise

        :Return: Is there something in the queue?
        :Rtype: ``bool``
        """
        return bool(self._queue)

    def __contains__(self, job_id):
        """
        Check if the passed job_id is currently enqueued

        :Return: Is it?
        :Rtype: ``bool``
        """
        return job_id in self._ids

    def __len__(self):
        """ Find queue length """
        return len(self._queue)

    def __iter__(self):
        """ Iterate over the queue until it's exhausted """
        try:
            while True:
                yield self.get()
        except IndexError:
            pass

    def put(self, job):
        """
        Put a job into the queue

        :Parameters:
          `job` : any
            The job to put in. The object must have an ``id`` attribute,
            which must be hashable.
        """
        self._ids.add(job.id)
        _heapq.heappush(self._queue, self._wrapper(job))

    def get(self):
        """
        Get the next job from the queue

        :Return: A job
        :Rtype: any

        :Exceptions:
          - `IndexError` : Queue was empty
        """
        job = _heapq.heappop(self._queue).job
        self._ids.remove(job.id)
        return job

    def peek(self):
        """
        Return the next job without removing it from the queue

        The job will still be wrapped in the wrapper_class container

        :Return: wrapped job
        :Rtype: any

        :Exceptions:
          - `IndexError` : Queue was empty
        """
        return self._queue[0]
