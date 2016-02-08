import binascii
import itertools

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
