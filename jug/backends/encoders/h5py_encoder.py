from .base import BaseEncoder

import binascii
import itertools
import h5py

import h5py.h5p
if not "set_file_image" in dir(h5py.h5p.PropFAID):
    import warnings
    warnings.warn("h5py imported, but h5py.h5p.PropFAID.set_file_image not supported. Ensure h5py > 2.6 and hdf > 1.8.9.")
    raise ImportError("h5py.h5p.PropFAID.set_file_image not supported.")

# See http://www.hdfgroup.org/HDF5/doc/H5.format.html
hdf5_format_signature = binascii.unhexlify("89 48 44 46 0d 0a 1a 0a".replace(" ", ""))

def hdf5_signature_indicies(max_userblock_size = None):
    """Yield search indicies for HDF5 format signature bytes, optionally limiting the maximum userblock size in target file."""

    yield 0
    for i in itertools.count():
        index = 512 * (2 ** i )

        if max_userblock_size and index > max_userblock_size:
            return

        yield index

def locate_hdf5_signature(target_file, max_userblock_size = None):
    """Locate hdf5 signature bytes in target file object.

    Seek though target file at hdf5 signature candidate locations, checking for signature.
    Optionally search up to a maximum userblock size.

    Args:
        target_file - File-like object supporting seek and read, function modified file location.
        max_userblock_size - Maximum target userblock size.

    Returns:
        Signature byte index, if found, otherwise None.
    """
    if isinstance(target_file, basestring):
        with open(target_file, 'r') as target_file_handle:
            return locate_hdf5_signature( target_file_handle, max_userblock_size )

    for search_index in hdf5_signature_indicies(max_userblock_size):
        target_file.seek( search_index )
        test_bytes = target_file.read(len(hdf5_format_signature))

        if test_bytes == hdf5_format_signature:
            return search_index
        elif len(test_bytes) != len(hdf5_format_signature):
            return None

def detect_hdf5_signature(target_file, max_userblock_size = None):
    """Search for hdf5 signature bytes in target file object.

    Seek though target file at hdf5 signature candidate locations, checking for signature.
    Optionally search up to a maximum userblock size.

    Args:
        target_file - File-like object supporting seek and read, function modified file location.
        max_userblock_size - Maximum target userblock size.

    Returns:
        True if signature found, False otherwise.
    """
    return locate_hdf5_signature(target_file, max_userblock_size) is not None

class H5PyEncoder(BaseEncoder):
    max_userblock_size = 4096

    @classmethod
    def can_load(cls, file):
        try:
            return detect_hdf5_signature( file, cls.max_userblock_size )
        finally:
            file.seek(0)

    @classmethod
    def load(cls, file):
        image = file.read()

        fapl = h5py.h5p.create(h5py.h5p.FILE_ACCESS)
        fapl.set_fapl_core(backing_store=False)
        fapl.set_file_image(image)

        fid = h5py.h5f.open("tf", h5py.h5f.ACC_RDONLY, fapl=fapl)
        return h5py.File(fid)

    @classmethod
    def can_dump(cls, obj):
        if type(obj) == h5py.File:
            return True

    @classmethod
    def dump(cls, obj, file):
        obj.flush()
        file.write(obj.fid.get_file_image())
