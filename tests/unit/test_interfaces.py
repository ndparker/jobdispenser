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

============================
 Tests for wolfe.interfaces
============================

Tests for wolfe.interfaces.
"""
from __future__ import absolute_import

__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"


from nose.tools import (  # pylint: disable = E0611
    assert_true, assert_false
)
from .._util import Bunch

from wolfe import interfaces as _interfaces

# pylint: disable = C0111, C1001, W0232


# test interfaces
class IF1(object):
    pass


class IF2(object):
    pass


class IF3(object):
    pass


class IF4(IF2):
    pass


class IF5:
    pass


def test_implements_no_interface():
    """ interfaces.implements handles non-implementing objects """
    assert_false(_interfaces.implements(Bunch(), 'lalal'))


def test_implements_no_interface_2():
    """ interfaces.implements handles empty interface list  """
    assert_false(_interfaces.implements(Bunch()))


def test_implements_all_ok():
    """ interfaces.implements finds all interfaces """
    assert_true(_interfaces.implements(Bunch(__implements__=[
        IF1, IF2
    ]), IF1, IF2))


def test_implements_sub_ok():
    """ interfaces.implements finds subclassed interfaces """
    assert_true(_interfaces.implements(Bunch(__implements__=[
        IF1, IF4
    ]), IF1, IF2))


def test_implements_sub_nok():
    """ interfaces.implements rejects super interfaces """
    assert_false(_interfaces.implements(Bunch(__implements__=[
        IF1, IF2
    ]), IF1, IF4))


def test_implements_missing_fail():
    """ interfaces.implements rejects missing interfaces """
    assert_false(_interfaces.implements(Bunch(__implements__=[
        IF1, IF4
    ]), IF1, IF2, IF3))


def test_implements_extra_ok():
    """ interfaces.implements accepts extra interfaces """
    assert_true(_interfaces.implements(Bunch(__implements__=[
        IF1, IF4, IF3
    ]), IF1, IF2))


def test_implements_accept_oldstyle():
    """ interfaces.implements accepts old-style interface classes """
    assert_true(_interfaces.implements(Bunch(__implements__=[
        IF1, IF5
    ]), IF5))


def test_implements_reject_nonclasses():
    """ interfaces.implements rejects non-class interfaces """
    assert_false(_interfaces.implements(Bunch(__implements__=[
        1, IF1
    ]), 1))