import h5py
import pickle
import copy_reg

__all__ = [
    "hdf5_format_signature", "hdf5_signature_indicies", "locate_hdf5_signature", "detect_hdf5_signature",
    "H5TemporaryFile",
]

# Disable h5py.File pickling by registering __reduce__ function to raise PicklingError
def _reduce_error(f):
    """Raise PicklingError on reduction operation."""
    raise pickle.PicklingError("Unable to pickle type: %s" % type(f))

copy_reg.pickle(h5py.File, _reduce_error)

from .detection import (hdf5_format_signature, hdf5_signature_indicies, locate_hdf5_signature, detect_hdf5_signature)
from .temp import H5TemporaryFile
