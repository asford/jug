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

from six import BytesIO
import six
import gzip

__all__ = ['encode', 'decode', 'encode_to', 'decode_from', 'available_encoders']

available_encoders = []
try:
    from .encoders.h5py_encoder import H5PyEncoder
    available_encoders.append( H5PyEncoder() )
except ImportError:
    pass

try:
    from .encoders.numpy_encoder import NumpyEncoder
    available_encoders.append( NumpyEncoder() )
except ImportError:
    pass

from .encoders.pickle_encoder import PickleEncoder

available_encoders.append(PickleEncoder())

def encode(obj):
    """Encode object to bytes.

    Encode object via first available encoder to bytes.

    Parameters
    ----------
      obj : Pickle-able or encodable object.

    Returns
    -------
      s : bytes

    See
    ---
      `decode`
    """
    output = BytesIO()
    encode_to(obj, output)
    return output.getvalue()

def encode_to(obj, stream):
    """Encode object to output stream.

    Encode object via first available encoder to output stream.

    Parameters
    ----------
      obj : Pickle-able or encodable object.
      stream : File-like object.

    Returns
    -------
      s : bytes

    See
    ---
      `decode`
    """
    stream = gzip.GzipFile(fileobj=stream, mode="wb")

    for e in available_encoders:
        if e.can_dump(obj):
            logger.debug("Resolved encoder: %s obj: %s", e, obj)
            e.dump(obj, stream)
            stream.flush()
            return
    else:
        raise ValueError("No valid encoder for obj.", obj)

def decode(s):
    '''Decode object from bytes.

    Reverses `encode`.

    Parameters
    ----------
      s : bytes representation of object

    Returns
    -------
      obj : the object
    '''
    return decode_from(BytesIO(s))

def decode_from(stream):
    '''Decode object from stream.

    Decodes the object from the stream ``stream``

    Parameters
    ----------
    stream : file-like object

    Returns
    -------
    obj : decoded object
    '''
    stream = gzip.GzipFile(fileobj=stream, mode="rb")

    for e in available_encoders:
        if e.can_load(stream):
            logger.debug("Resolved decoder: %s", e)
            return e.load(stream)
    else:
        raise ValueError("No valid decoder for stream.")
