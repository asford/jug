from os import path

import six
import pickle

from nose.tools import with_setup, assert_raises
from nose import SkipTest

import jug.backends.redis_store
import jug.backends.file_store
import jug.backends.dict_store
from jug.backends.redis_store import redis


def test_stores():
    def load_get(store):
        try:
            assert len(list(store.list())) == 0
            key = six.b('jugisbestthingever')
            assert not store.can_load(key)
            object = list(range(232))
            store.dump(object, key)
            assert store.can_load(key)
            assert store.load(key) == object
            assert len(list(store.list())) == 1
            store.remove(key)
            assert not store.can_load(key)
            store.close()
        except redis.ConnectionError:
            raise SkipTest()


    def lock(store):
        try:
            assert len(list(store.listlocks())) == 0
            key = six.b('jugisbestthingever')
            lock = store.getlock(key)
            assert not lock.is_locked()
            assert lock.get()
            assert not lock.get()
            lock2 = store.getlock(key)
            assert not lock2.get()
            assert len(list(store.listlocks())) == 1
            lock.release()
            assert lock2.get()
            lock2.release()
            store.close()
        except redis.ConnectionError:
            raise SkipTest()

    def lock_remove(store):
        try:
            assert len(list(store.listlocks())) == 0
            key = six.b('jugisbestthingever')
            lock = store.getlock(key)
            assert not lock.is_locked()
            assert lock.get()
            assert not lock.get()
            assert len(list(store.listlocks())) == 1
            store.remove_locks()
            assert len(list(store.listlocks())) == 0
            store.close()
        except redis.ConnectionError:
            raise SkipTest()
    stores = [
        lambda: jug.backends.file_store.file_store('jug_test_store'),
            jug.backends.dict_store.dict_store,
        lambda: jug.redis_store.redis_store('redis://'),
            ]
    teardowns = [
        lambda: jug.backends.file_store.file_store.remove_store("jug_test_store"),
        None,
        None,
    ]

    functions = (load_get, lock, lock_remove)

    for f in functions:
        for store,tear in zip(stores,teardowns):
            f.teardown = tear
            yield f, store()

@with_setup(teardown=lambda: jug.backends.file_store.file_store.remove_store("jug_test_numpy_store"))
def test_numpy_store():

    try:
        import numpy as np
    except ImportError:
        raise SkipTest()

    store = jug.backends.file_store.file_store("jug_test_numpy_store")

    key = 'mykey'
    arr = (np.arange(100) % 17).reshape((10, 10))

    store.dump(arr, key)
    arr2 = store.load(key)

    assert np.all(arr2 == arr)

    store.remove(key)
    store.close()

@with_setup(teardown=lambda: jug.backends.file_store.file_store.remove_store("jug_test_h5py_store"))
def test_h5py_store():
    store = jug.backends.file_store.file_store("jug_test_h5py_store")
    store.create()

    try:
        import h5py
        import numpy
    except ImportError:
        raise SkipTest()

    key = 'mykey'

    db = h5py.File( path.join( store.tempdir(), "temp_db" ), "w")
    db.create_dataset("test_array", data=numpy.arange(100))

    store.dump( db, key)
    result_db = store.load(key)

    assert result_db.filename == store._getfname(key)
    assert result_db.filename != db.filename
    assert result_db.mode == 'r'

    assert numpy.all( db["test_array"][:] == result_db["test_array"][:])

    invalid_store = jug.backends.dict_store.dict_store()
    assert_raises(pickle.PicklingError, invalid_store.dump, db, key)
