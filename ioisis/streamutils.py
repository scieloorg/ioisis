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


class LineSplitError(Exception):
    pass


class LineSplittedBytesStreamWrapper:

    def __init__(self, substream, line_len, newline):
        self.substream = substream
        self.line_len = line_len
        self.newline = newline
        self.rnext_eol = line_len
        self.writing = False

    def _check_eol(self):
        if self.substream.read(len(self.newline)) != self.newline:
            raise LineSplitError("Invalid record line splitting")

    def read(self, count=None):
        result = []
        remaining = float("inf") if count is None else count
        while remaining > 0:
            expected_len = min(self.rnext_eol, remaining)
            data = self.substream.read(expected_len)
            data_len = len(data)
            result.append(data)
            remaining -= data_len
            if self.rnext_eol == data_len:
                self._check_eol()
                self.rnext_eol = self.line_len
            else:
                self.rnext_eol -= data_len
                break
        return b"".join(result)

    def write(self, data):
        self.writing = True
        result = remaining = len(data)
        while data:
            buff_len = min(self.rnext_eol, remaining)
            buff, data = (data[:buff_len], data[buff_len:])
            self.substream.write(buff)
            remaining -= buff_len
            if self.rnext_eol == buff_len:
                self.substream.write(self.newline)
                self.rnext_eol = self.line_len
            else:
                self.rnext_eol -= buff_len
                break
        return result

    def close(self):
        if self.rnext_eol != self.line_len:
            if self.writing:
                self.substream.write(self.newline)
            else:
                self._check_eol()
        self.substream = None

    tellable = lambda self: self.substream.tellable()

    def tell(self):
        line_no, col_no = divmod(self.substream.tell(),
                                 self.line_len + len(self.newline))
        return line_no * self.line_len + col_no

    seekable = lambda self: self.substream.seekable()

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            if offset < 0:
                raise ValueError("Negative offset")
            line_no, col_no = divmod(offset, self.line_len)
            line_start = line_no * (self.line_len + len(self.newline))
            self.substream.seek(line_start, whence)
            self.rnext_eol = self.line_len
            self.read(col_no)
            return offset
        elif whence == io.SEEK_CUR:
            return self.seek(self.substream.tell() + offset, io.SEEK_SET)
        elif whence == io.SEEK_END:
            if not self.finished:
                self.read()  # Just to reach the end of stream
            return self.seek(max(0, self.substream.tell() + offset),
                             io.SEEK_SET)
        raise ValueError("Invalid whence")


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
