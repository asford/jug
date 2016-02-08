# -*- coding: utf-8 -*-
# Copyright (C) 2009-2012, Luis Pedro Coelho <luis@luispedro.org>
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


import os
import contextlib

from .task import Task, Tasklet, TaskBase, TaskGenerator, value, tasks_for_value
from .hash import new_hash_object, hash_update
import contextlib

__all__ = [
    'timed_path',
    'identity',
    'CustomHash',
    'lazy_load'
    ]

def _return_first(one, two):
    '''
    one = _return_first(one, two)

    Used internally to implement jug.util.timed_path
    '''
    return one

def timed_path(path):
    '''
    opath = timed_path(ipath)

    Returns a Task object that simply returns `path` with the exception that it uses the
    paths mtime (modification time) in the hash. Thus, if the file contents change, this triggers
    an invalidation of the results (which propagates).

    Parameters
    ----------
    ipath : str
        A filesystem path

    Returns
    -------
    opath : str
        A task equivalent to ``(lambda: ipath)``.
    '''
    mtime = os.stat_result(os.stat(path)).st_mtime
    return Task(_return_first, path, mtime)

def _identity(x):
    return x

def identity(x):
    '''
    x = identity(x)

    `identity` implements the identity function as a Task
    (i.e., value(identity(x)) == x)

    This seems pointless, but if x is, for example, a very large list, then
    using the output of this function might be much faster than using x directly.

    Parameters
    ----------
    x : any object

    Returns
    -------
    x : x
    '''
    if isinstance(x, TaskBase):
        return x
    t = Task(_identity, x)
    t.name = 'identity'
    return t

class CustomHash(object):
    '''
    value = CustomHash(obj, hash_function)

    Set a custom hash function

    This is an advanced feature and you can shoot yourself in the foot with it.
    Make sure you know what you are doing. In particular, hash_function should
    be a strong hash: ``hash_function(obj0) == hash_function(obj1)`` is taken
    to imply that ``obj0 == obj1``

    Parameters
    ----------
    obj : any object
    hash_function : function
        This should take your object and return a str
    '''
    def __init__(self, obj, hash_function):
        self.obj = obj
        self.hash_function = hash_function

    def __jug_hash__(self):
        return self.hash_function(self.obj)

    def __jug_value__(self):
        return value(self.obj)

def lazy_load(values):
    """Wrap contents in Task-like object which returns included tasks as
    dependencies, but does not load the subtasks before execution."""

    return LazyTaskContainer(values)

class LazyTaskContainer(TaskBase):
    def __init__(self, target):
        self.target = target

    def value(self):
        return self.target

    def dependencies(self):
        return tasks_for_value(self.target)

    def can_run(self, store = None):
        for dep in tasks_for_value(self.target):
            if not dep.is_loaded() and not dep.can_load(store):
                return False
        return True

    def can_load(self, store=None):
        for dep in tasks_for_value(self.target):
            if not dep.can_load(store):
                return False
        return True

    def is_loaded(self):
        return all( t.is_loaded() for t in tasks_for_value(self.target) )

    def unload(self):
        pass

    def _compute_set_hash(self):
        import six
        M = new_hash_object()
        M.update(six.b('LazyTask'))
        hash_update(M, [("target", self.target)])
        value = M.hexdigest().encode('utf-8')

        self.__jug_hash__ = lambda : value
        return value

    def __jug_hash__(self):
        """Calculate and return hash for this task.

        The results are cached, so the first call can be much slower than
        subsequent calls.
        """
        return self._compute_set_hash()

@contextlib.contextmanager
def lazy_value(task_or_value):
    """Context manager managing loading result from jug task.

    Manager load values from jug tasks or passes through values. Task is
    unloaded on close if it was not already loaded.
    """
    to_unload = [t for t in tasks_for_value(task_or_value) if not t.is_loaded()]

    yield value(task_or_value)

    for t in to_unload:
        t.unload()
