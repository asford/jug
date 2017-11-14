def _open_gzip_compressed(f):
    from gzip import GzipFile
    return GzipFile(filename=f) if isinstance(f, str) else GzipFile(fileobj=f)


def _open_bz2_compressed(f):
    from bz2file import BZ2File
    return BZ2File(f)

def _open_snappy_compressed(f):
    import snappy_stream

    return snappy_stream.SnappyReadWrapper(f, owns_source=True)

# bz2file used to support concatenated bz2 streams
_compressed_magic_bytes = {}
_compressed_magic_bytes[b"\x1f\x8b\x08"] = _open_gzip_compressed
_compressed_magic_bytes[b"\x42\x5a\x68"] = _open_bz2_compressed
_compressed_magic_bytes[b'\xff\x06\x00\x00sNaPpY'] = _open_snappy_compressed

def open_compressed(filename):
    """Open a possibly compressed file or stream as a decompressed file object.
    Checks for prefix bytes and returns a decompression stream if needed.
    filename - str filename or file object supporting seek
    """

    if isinstance(filename, str):
        file_object = open(filename, "rb")
    else:
        file_object = filename

    # Peek ahead for magic bytes
    current_stream_position = file_object.tell()
    file_prefix = file_object.read(max(len(b) for b in _compressed_magic_bytes))
    file_object.seek(current_stream_position)

    for magic_bytes in _compressed_magic_bytes:
        if file_prefix.startswith(magic_bytes):
            return _compressed_magic_bytes[magic_bytes](file_object)
    else:
        return file_object
