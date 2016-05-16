import h5py

import tempfile
import functools

__all__ = ["H5TemporaryFile",]

def H5Temporary_close(handle):
    """Wrapper generator to support File.close for H5TemporaryFile files."""

    base_close = handle.close

    @functools.wraps(base_close)
    def close_wrapper():
        base_close()
        handle.backing_file.close()

    return close_wrapper

def H5TemporaryFile( suffix='', prefix='tmp', dir=None, delete=True ):
    """Create and return a temporary h5py.File backed by NamedTemporaryFile.

    Create a temporary file via NamedTemporaryFile and open h5py file over
    temporary file.  File is automatically deleted when closed if delete is
    True, as lifetime of the returned NamedTemporaryFile is tied to returned
    File via 'backing_file' property and overloaded File.close method.
    Args are as for tempfile.NamedTemporaryFile

    Args:
        suffix - See tempfile.NamedTemporaryFile
        prefix - See tempfile.NamedTemporaryFile
        dir    - See tempfile.NamedTemporaryFile
        delete - See tempfile.NamedTemporaryFile

    Returns:
        Open h5py.File handle.
    """


    backing_file = tempfile.NamedTemporaryFile( mode='w+b', suffix=suffix, prefix=prefix, dir=dir, delete=delete )

    h5_handle = h5py.File( backing_file.name, mode="w" )
    h5_handle.backing_file = backing_file
    h5_handle.close = H5Temporary_close( h5_handle )

    return h5_handle
