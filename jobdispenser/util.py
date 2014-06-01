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
 Misc Utilities
================

Misc utilities.
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"


class Version(tuple):
    """
    Represents the package version

    :IVariables:
      `major` : ``int``
        The major version number

      `minor` : ``int``
        The minor version number

      `patch` : ``int``
        The patch level version number

      `is_dev` : ``bool``
        Is it a development version?

      `revision` : ``int``
        Internal revision
    """

    def __new__(cls, versionstring, is_dev, revision):
        """
        Construction

        :Parameters:
          `versionstring` : ``str``
            The numbered version string (like ``"1.1.0"``)
            It should contain at least three dot separated numbers

          `is_dev` : ``bool``
            Is it a development version?

          `revision` : ``int``
            Internal revision

        :Return: New version instance
        :Rtype: `version`
        """
        # pylint: disable = W0613
        tup = []
        versionstring = versionstring.strip()
        if versionstring:
            for item in versionstring.split('.'):
                try:
                    item = int(item)
                except ValueError:
                    pass
                tup.append(item)
        while len(tup) < 3:
            tup.append(0)
        return tuple.__new__(cls, tup)

    def __init__(self, versionstring, is_dev, revision):
        """
        Initialization

        :Parameters:
          `versionstring` : ``str``
            The numbered version string (like ``1.1.0``)
            It should contain at least three dot separated numbers

          `is_dev` : ``bool``
            Is it a development version?

          `revision` : ``int``
            Internal revision
        """
        # pylint: disable = W0613
        super(Version, self).__init__()
        self.major, self.minor, self.patch = self[:3]
        self.is_dev = bool(is_dev)
        self.revision = int(revision)

    def __repr__(self):
        """
        Create a development string representation

        :Return: The string representation
        :Rtype: ``str``
        """
        return "%s.%s(%r, is_dev=%r, revision=%r)" % (
            self.__class__.__module__,
            self.__class__.__name__,
            ".".join(map(str, self)),
            self.is_dev,
            self.revision,
        )

    def __str__(self):
        """
        Create a version like string representation

        :Return: The string representation
        :Rtype: ``str``
        """
        return "%s%s" % (
            ".".join(map(str, self)),
            ("", "-dev-r%d" % self.revision)[self.is_dev],
        )

    def __unicode__(self):
        """
        Create a version like unicode representation

        :Return: The unicode representation
        :Rtype: ``unicode``
        """
        return str(self).decode('ascii')


def find_public(space):
    """
    Determine all public names in space

    :Parameters:
      `space` : ``dict``
        Name space to inspect

    :Return: List of public names
    :Rtype: ``list``
    """
    if space.has_key('__all__'):
        return list(space['__all__'])
    return [key for key in space.keys() if not key.startswith('_')]
