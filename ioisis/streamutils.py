from functools import update_wrapper
from inspect import isgeneratorfunction, signature
import io


def should_be_file(file_argname, mode="rb"):
    """Decorator to enforce the given argument is a file-like object.
    If it isn't, it will be seen as a filename,
    and it'll be replaced by the open file with the given mode.
    """
    def decorator(func):
        if isgeneratorfunction(func):
            def wrapper(*args, **kwargs):
                bound_args = signature(func).bind(*args, **kwargs)
                file_arg = bound_args.arguments[file_argname]
                if hasattr(file_arg, "read"):  # Already a file, nothing to do
                    yield from func(*args, **kwargs)
                    return
                with open(file_arg, mode) as file_obj:  # "Cast" name to file
                    bound_args.arguments[file_argname] = file_obj
                    yield from func(*bound_args.args, **bound_args.kwargs)
        else:
            raise NotImplementedError
        return update_wrapper(wrapper, func)
    return decorator


class TightBufferReadOnlyBytesStreamWrapper:

    def __init__(self, substream):
        self.substream = substream
        self.buffer = b""
        self.offset = 0
        self.finished = False

    def read(self, size=None):
        if size is None or size < 0:
            self.buffer += self.substream.read()
            result = self.buffer[self.offset:]
            self.finished = True
        else:
            expected_offset = self.offset + size
            missing = expected_offset - len(self.buffer)
            if missing > 0:
                self.buffer += self.substream.read(missing)
                if len(self.buffer) < expected_offset:
                    self.finished = True
            result = self.buffer[self.offset:expected_offset]
        self.offset += len(result)
        return result

    close = lambda self: None  # Required to be a file-like object
    tellable = lambda self: True
    tell = lambda self: self.offset
    seekable = lambda self: True

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            if offset < 0:
                raise ValueError("Negative offset")
            self.offset = offset
        elif whence == io.SEEK_CUR:
            self.offset = max(0, self.offset + offset)
        elif whence == io.SEEK_END:
            if not self.finished:
                self.read()  # Just to reach the end of stream
            self.offset = max(0, len(self.buffer) + offset)
        else:
            raise ValueError("Invalid whence")
        return self.offset
