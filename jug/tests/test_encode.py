from six import BytesIO
import six
from jug.backends.encode import encode, decode
import numpy as np

def test_encode():
    assert decode(encode(None)) is None
    assert decode(encode([])) == []
    assert decode(encode(list(range(33)))) == list(range(33))

def test_numpy():
    assert np.all(decode(encode(np.arange(33))) == np.arange(33))

class Derived(np.ndarray):
    def __new__(cls, value):
        return np.ndarray.__new__(cls, value)

def test_numpy_derived():
    a = Derived([1,2,3])
    assert type(decode(encode(a))) == type(a)
