"""Custom construct subclasses."""
from contextlib import closing

from construct import Adapter, Subconstruct

from .streamutils import LineSplittedBytesStreamWrapper


DEFAULT_LINE_LEN = 80
DEFAULT_NEWLINE = b"\n"


class IntInASCII(Adapter):
    """Adapter for Bytes to use it as ASCII numbers."""
    def _decode(self, obj, context, path):
        return int(obj, base=10)

    def _encode(self, obj, context, path):
        length = self.subcon.sizeof(**context)
        return (b"%d" % obj).zfill(length)


class LineSplitRestreamed(Subconstruct):
    """Alternative to Restreamed
    that parses a "line splitted" data,
    builds the lines appending the ``newline`` character/string,
    and works properly with a last incomplete chunk.
    """
    def __init__(self, subcon, line_len=DEFAULT_LINE_LEN,
                 newline=DEFAULT_NEWLINE):
        super().__init__(subcon)
        self.line_len = line_len
        self.newline = newline

    def _parse(self, stream, context, path):
        with closing(LineSplittedBytesStreamWrapper(
            substream=stream,
            line_len=self.line_len,
            newline=self.newline,
        )) as stream2:
            return self.subcon._parsereport(stream2, context, path)

    def _build(self, obj, stream, context, path):
        with closing(LineSplittedBytesStreamWrapper(
            substream=stream,
            line_len=self.line_len,
            newline=self.newline,
        )) as stream2:
            self.subcon._build(obj, stream2, context, path)
        return obj

    def _sizeof(self, context, path):
        n = self.subcon._sizeof(context, path)
        return n + (n // self.line_len + 1) * len(self.newline),


class Unnest(Adapter):
    """Adapter for dict-like containers to unnest (embed) substructures."""
    def __init__(self, names, subcon):
        super().__init__(subcon)
        self.names = list(names)

    def _decode(self, obj, context, path):
        result = obj.copy()
        for name in self.names:
            if name in result:
                result.update(result.pop(name))
        return result

    def _encode(self, obj, context, path):
        result = obj.copy()
        for name in self.names:
            result[name] = obj
        return result
