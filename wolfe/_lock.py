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

=======
 Locks
=======

Lock containers.
"""
if __doc__:  # pragma: no cover
    # pylint: disable = redefined-builtin
    __doc__ = __doc__.encode('ascii').decode('unicode_escape')
__author__ = r"Andr\xe9 Malo".encode('ascii').decode('unicode_escape')
__docformat__ = "restructuredtext en"

import operator as _op

from ._exceptions import LockConflict
from . import interfaces as _interfaces


class Lock(object):
    """
    Lock container

    :See: `interfaces.LockInterface`
    """
    __implements__ = [_interfaces.LockInterface]

    __slots__ = ('name', 'exclusive')

    def __init__(self, name, exclusive=True):
        """
        Initialization

        :Parameters:
          `name` : ``str``
            Lock name

          `exclusive` : ``bool``
            Does this lock has to be exclusive? Default: true
        """
        self.name = name
        self.exclusive = bool(exclusive)


def validate(locks):
    """
    Validate locks and order by name

    :Parameters:
      `locks` : iterable
        List of locks (``[LockInterface, ...]``). Empty iterables or ``None``
        result in an empty result list

    :Return: Validated and ordered locks
    :Rtype: ``list``

    :Exceptions:
      - `LockConflict` : Conflicting locks were provided
    """
    if locks is None:
        return []

    locks = list(locks)
    result = []
    # .reverse() ensures sorting stability, while reverse=True doesn't. Hrm.
    locks.sort(key=_op.attrgetter('name'))
    locks.reverse()

    # initial value of last contains a unique object which lock.name never can
    # provide
    last = type('unset', (object,), {})(), True
    while locks:
        lock = locks.pop()
        excl = bool(lock.exclusive)
        if last[0] == lock.name:
            if last[1] != excl:
                raise LockConflict(lock.name)
            continue  # pragma: no cover (coverage.py doesn't get this line)
        last = lock.name, excl
        result.append(lock)

    return result
