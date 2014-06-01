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

==================================================
 JobDispenser - A Reliable Task Management System
==================================================

JobDispenser - A Reliable Task Management System.
"""
__author__ = u"Andr\xe9 Malo"
__docformat__ = "restructuredtext en"
__license__ = "Apache License, Version 2.0"
__version__ = ('0.9.9.8', False, 4809)

from jobdispenser import util as _util
from jobdispenser._exceptions import * # pylint: disable = W0401, W0614, W0622

#: Version of the jobdispenser package
#:
#: :Type: `jobdispenser.util.Version`
version = _util.Version(*__version__)

__all__ = _util.find_public(globals())
del _util
