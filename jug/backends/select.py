# -*- coding: utf-8 -*-
# Copyright (C) 2008-2010, Luis Pedro Coelho <luis@luispedro.org>
# vim: set ts=4 sts=4 sw=4 expandtab smartindent:
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.

import logging
logger = logging.getLogger(__name__)

from .redis_store import redis_store
from .gcs_redis_store import gcs_redis_store
from .file_store import file_store
from .dict_store import dict_store

def select(jugdir):
    '''
    store = select(jugdir)

    Returns a store object appropriate for `jugdir`

    Parameters
    ----------
      jugdir : string
            representation of jugdir.
            Alternatively, if not a string, a data store

    Returns
    -------
      store : A jug data store
    '''
    logger.debug("Select jugdir=%r", jugdir)
    if type(jugdir) != str:
        return jugdir
    if jugdir.startswith('redis:'):
        return redis_store(jugdir)
    if jugdir == 'dict_store':
        return dict_store()
    if jugdir.startswith('dict_store:'):
        return dict_store(jugdir[len('dict_store:'):])
    if jugdir.startswith('gcs+redis:'):
        return gcs_redis_store(connection_string = jugdir)
    return file_store(jugdir)

