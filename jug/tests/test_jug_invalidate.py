from nose.tools import with_setup
import jug.jug
import jug.task
from jug.task import Task
from jug.backends.dict_store import dict_store
from jug.options import Options, default_options
from jug.tests.utils import simple_execute
from jug.tests.task_reset import task_reset
import random
jug.jug.silent = True


@task_reset
def test_jug_invalidate():
    def setAi(i):
        A[i] = True
    N = 1024
    A = [False for i in range(N)]
    setall = [Task(setAi, i) for i in range(N)]
    store = dict_store()
    jug.task.Task.store = store
    for t in setall: t.run()

    opts = Options(default_options)
    opts.invalid_name = setall[0].name
    jug.jug.invalidate(store, opts)
    assert not list(store.store.keys()), list(store.store.keys())
    jug.task.Task.store = dict_store()

@task_reset
def test_complex():
    store, space = jug.jug.init('jug/tests/jugfiles/tasklets.py', 'dict_store')
    simple_execute()

    store, space = jug.jug.init('jug/tests/jugfiles/tasklets.py', store)
    opts = Options(default_options)
    opts.invalid_name = space['t'].name
    h = space['t'].hash()
    assert store.can_load(h)
    jug.jug.invalidate(store, opts)
    assert not store.can_load(h)

@task_reset
def test_cleanup():
    store, space = jug.jug.init('jug/tests/jugfiles/tasklets.py', 'dict_store')
    simple_execute()

    h = jug.task.alltasks[-1].hash()
    l = store.getlock(h)

    del jug.task.alltasks[-1]
    assert store.can_load(h)
    assert not l.is_locked()

    l.get()
    assert store.can_load(h)
    assert l.is_locked()

    opts = Options(default_options)
    opts.cleanup_locks_only = True
    jug.jug.cleanup(store, opts)
    assert store.can_load(h)
    assert not l.is_locked()

    opts.cleanup_locks_only = False
    jug.jug.cleanup(store, opts)
    assert not store.can_load(h)
    assert not l.is_locked()
