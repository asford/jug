# -*- coding: utf-8 -*-
# Copyright (C) 2008-2014, Luis Pedro Coelho <luis@luispedro.org>
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

from abc import ABCMeta, abstractmethod, abstractproperty

import logging
logger = logging.getLogger(__name__)

from six.moves import cPickle as pickle
from six import BytesIO
import six
import zlib

__all__ = ['encode', 'decode', 'encode_to', 'decode_from']

class BaseEncoder(object):
    __metaclass__ = ABCMeta

    @abstractproperty
    def prefix(self):
        raise NotImplementedError("prefix")

    @abstractmethod
    def decode_from(self, stream):
        """Decode object from stream."""
        raise NotImplementedError("decode_from")

    @abstractmethod
    def can_encode(self, target):
        """Return True if can encode target, else False."""
        raise NotImplementedError("can_encode")

    @abstractmethod
    def write(self, target, stream):
        """Encode target object to stream."""
        raise NotImplementedError("write")

class NDArrayEncoder(BaseEncoder):
    prefix = six.b('N')

    @classmethod
    def can_encode(cls, target):
        return type(target) == numpy.ndarray

    @classmethod
    def write(cls, target, stream):
        assert isinstance( target, numpy.ndarray )

        stream.write(cls.prefix)
        numpy.save(stream, target)

    @classmethod
    def decode_from(cls, stream):
        return numpy.load(stream)

class PickleEncoder(BaseEncoder):
    prefix = six.b('P')

    @classmethod
    def can_encode(cls, obj):
        return True

    @classmethod
    def write(cls, target, stream):
        stream.write(cls.prefix)
        pickle.dump( target, stream, 2 )

    @classmethod
    def decode_from(cls, stream):
        return pickle.load(stream)

encoder_set = []
try:
    import numpy
    encoder_set.append( NDArrayEncoder() )
except ImportError:
    pass

encoder_set.append(PickleEncoder())

def encode(obj):
    '''
    s = encode(obj)

    Return a string (byte-array) representation of obj.

    Parameters
    ----------
      obj : Any thing that is pickle()able

    Returns
    -------
      s : string (byte array).

    See
    ---
      `decode`
    '''
    output = BytesIO()
    encode_to(obj, output)
    return output.getvalue()

def encode_to(obj, stream):
    '''
    encode_to(obj, stream)

    Encodes the obj into the stream ``stream``

    Parameters
    ----------
    obj : Any object
    stream : file-like object
    '''
    if obj is None:
        logger.debug("encode_to obj: None")
        return

    stream = compress_stream(stream)
    for e in encoder_set:
        if e.can_encode(obj):
            logger.debug("Resolved encoder: %s obj: %s", e, obj)
            e.write(obj, stream)
            break
    else:
        raise ValueError("No valid encoder for obj.", obj)

    stream.flush()

class compress_stream(object):
    def __init__(self, stream):
        self.stream = stream
        self.C = zlib.compressobj()

    def write(self, s):
        self.stream.write(self.C.compress(s))

    def flush(self):
        self.stream.write(self.C.flush())
        self.stream.flush()

class decompress_stream(object):
    def __init__(self, stream, block=8192):
        self.stream = stream
        self.D = zlib.decompressobj()
        self.block = block
        self.lastread = six.b('')
        self.queue = six.b('')

    def read(self, nbytes):
        res = six.b('')
        if self.queue:
            if len(self.queue) >= nbytes:
                res = self.queue[:nbytes]
                self.queue = self.queue[nbytes:]
                return res
            res = self.queue
            self.queue = b''

        if self.D.unconsumed_tail:
            res += self.D.decompress(self.D.unconsumed_tail, nbytes - len(res))
        while len(res) < nbytes:
            buf = self.stream.read(self.block)
            if not buf:
                res += self.D.flush()
                break
            res += self.D.decompress(buf, nbytes - len(res))
        self.lastread = res
        return res

    def seek(self, offset, whence):
        if whence != 1:
            raise NotImplementedError
        while offset > 0:
            nbytes = min(offset, self.block)
            self.read(nbytes)
            offset -= nbytes
        if offset < 0:
            if offset > len(self.lastread):
                raise ValueError('seek too far')
            skip = len(self.lastread) + offset
            self.queue = self.lastread[skip:]

    def readline(self):
        qi = self.queue.find(six.b('\n'))
        if qi >= 0:
            qi += 1
            res = self.queue[:qi]
            self.queue = self.queue[qi:]
            return res
        line = six.b('')
        while True:
            block = self.read(self.block)
            if not block:
                return line
            ln = block.find(six.b('\n'))
            if ln == -1:
                line += block
            else:
                ln += 1
                line += block[:ln]
                self.seek(ln-len(block), 1)
                return line
        return line

def decode(s):
    '''
    object = decode(s)

    Reverses `encode`.

    Parameters
    ----------
      s : a string representation of the object.

    Returns
    -------
      object : the object
    '''
    return decode_from(BytesIO(s))

def decode_from(stream):
    '''
    object = decode_from(stream)

    Decodes the object from the stream ``stream``

    Parameters
    ----------
    stream : file-like object

    Returns
    -------
    object : decoded object
    '''
    stream = decompress_stream(stream)
    prefix = stream.read(1)
    if not prefix:
        logger.debug("decode_from without prefix. return None")
        return None

    for e in encoder_set:
        if prefix == e.prefix:
            logger.debug("Resolved decoder: %s", e)
            return e.decode_from(stream)
    else:
        raise IOError("jug.backend.decode_from: unknown prefix '%s'" % prefix)
