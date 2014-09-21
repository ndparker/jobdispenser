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
 Tests for wolfe._util
=======================

Tests for wolfe._util
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"

import types as _types

from nose.tools import (  # pylint: disable = E0611
    assert_equals,
)

from wolfe import _util


def test_find_public_symbols():
    """ find_public finds all non underscored symbols """
    mod = _types.ModuleType('lala')
    mod.a = 1
    mod._b = 2  # pylint: disable = W0212

    assert_equals(_util.find_public(vars(mod)), ['a'])


def test_find_public_all():
    """ find_public passes __all__ """
    mod = _types.ModuleType('lala')
    mod.__all__ = ['_b', 'c']

    assert_equals(_util.find_public(vars(mod)), ['_b', 'c'])