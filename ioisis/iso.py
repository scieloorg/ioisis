"""Model for the ISIS ISO2709-based file format.

This file format specification can be found at:

https://wiki.bireme.org/pt/img_auth.php/5/5f/2709BR.pdf
"""
from collections import defaultdict
from contextlib import closing
from functools import partial
from itertools import accumulate
import re

from construct import Adapter, Array, Bytes, Check, CheckError, Computed, \
                      Const, Default, Embedded, Rebuild, Select, Struct, \
                      Subconstruct, Terminated, this

from .common import should_be_file


DEFAULT_FIELD_TERMINATOR = b"#"
DEFAULT_RECORD_TERMINATOR = b"#"
DEFAULT_ISO_ENCODING = "cp1252"

LABEL_LEN = 24
TAG_LEN = 3
DEFAULT_LEN_LEN = 4
DEFAULT_POS_LEN = 5
DEFAULT_CUSTOM_LEN = 0

# Only for building
DEFAULT_LINE_LEN = 80
DEFAULT_NEWLINE = b"\n"


# TODO: remove this in v0.2 as it's no longer required
clear_cr_lf = partial(re.compile(b"[\r\n]").sub, b"")


class IntInASCII(Adapter):
    """Adapter for Bytes to use it as BCD (Binary-coded decimal)."""
    def _decode(self, obj, context, path):
        return int(obj, base=10)

    def _encode(self, obj, context, path):
        length = self.subcon.sizeof(**context)
        return (b"%d" % obj).zfill(length)


class CheckTrimSuffix(Adapter):
    """Adapter for Bytes to check/insert/remove a given suffix,
    making it possible to check both the string suffix and size.
    The subcon size must include the suffix length.
    """
    def __init__(self, subcon, suffix):
        self._suffix = suffix
        self._suffix_len = len(suffix)
        super(CheckTrimSuffix, self).__init__(subcon)

    def _decode(self, obj, context, path):
        if not obj.endswith(self._suffix):
            raise CheckError("Missing the %s suffix." % repr(self._suffix))
        return obj[:-self._suffix_len]

    def _encode(self, obj, context, path):
        return obj + self._suffix


class LineSplittedBytesIO:

    def __init__(self, substream, line_len, newline):
        self.substream = substream
        self.line_len = line_len
        self.newline = newline
        self.wbuffer = b""
        self.rnext_eol = line_len

    def _check_eol(self):
        if self.substream.read(len(self.newline)) != self.newline:
            raise CheckError("Invalid record line splitting")

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
        self.wbuffer += data
        result = len(data)
        while len(self.wbuffer) >= self.line_len:
            data, self.wbuffer = (self.wbuffer[:self.line_len],
                                  self.wbuffer[self.line_len:])
            self.substream.write(data)
            self.substream.write(self.newline)
        return result

    def close(self):
        if self.rnext_eol != self.line_len:
            self._check_eol()
        if self.wbuffer:
            self.substream.write(self.wbuffer)
            self.substream.write(self.newline)


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
        with closing(LineSplittedBytesIO(
            substream=stream,
            line_len=self.line_len,
            newline=self.newline,
        )) as stream2:
            return self.subcon._parsereport(stream2, context, path)

    def _build(self, obj, stream, context, path):
        with closing(LineSplittedBytesIO(
            substream=stream,
            line_len=self.line_len,
            newline=self.newline,
        )) as stream2:
            self.subcon._build(obj, stream2, context, path)
        return obj

    def _sizeof(self, context, path):
        n = self.subcon._sizeof(context, path)
        return n + (n // self.line_len + 1) * len(self.newline),


def line_split_restreamed(
    subcon,
    line_len=DEFAULT_LINE_LEN,
    newline=DEFAULT_NEWLINE
):
    import warnings
    warnings.warn("ioisis.iso.line_split_restreamed is deprecated. "
                  "Use ioisis.iso.LineSplitRestreamed instead",
                  DeprecationWarning)
    return LineSplitRestreamed(subcon, line_len, newline)


def create_record_struct(
    field_terminator=DEFAULT_FIELD_TERMINATOR,
    record_terminator=DEFAULT_RECORD_TERMINATOR,
):
    """Create a construct parser/builder for a whole record object."""
    ft_len = len(field_terminator)
    rt_len = len(record_terminator)
    return Struct(
        # Build time pre-computed information
        "_build_len_list" / Computed(
            lambda this: None if "fields" not in this else
                [len(field) + ft_len for field in this.fields]
        ),
        "_build_pos_list" / Computed(
            lambda this: None if "fields" not in this else
                list(accumulate([0] + this._build_len_list))
        ),
        "_build_dir_len" / Computed(
            lambda this: None if "fields" not in this else
                len(this.fields) * (
                    TAG_LEN
                    + this.get("len_len", DEFAULT_LEN_LEN)
                    + this.get("pos_len", DEFAULT_POS_LEN)
                    + this.get("custom_len", DEFAULT_CUSTOM_LEN)
                )
        ),

        # Record label/header
        Embedded(Struct(
            "total_len" / Rebuild(IntInASCII(Bytes(5)),
                lambda this: LABEL_LEN
                    + this._build_dir_len
                    + ft_len
                    + this._build_pos_list[-1]  # Fields length
                    + rt_len
            ),
            "status" / Default(Bytes(1), b"0"),
            "type" / Default(Bytes(1), b"0"),
            "custom_2" / Default(Bytes(2), b"00"),
            "coding" / Default(Bytes(1), b"0"),
            "indicator_count" / Default(IntInASCII(Bytes(1)), 0),
            "identifier_len" / Default(IntInASCII(Bytes(1)), 0),
            "base_addr" / Rebuild(IntInASCII(Bytes(5)),
                                  LABEL_LEN + this._build_dir_len
                                            + ft_len),
            "custom_3" / Default(Bytes(3), b"000"),
            Embedded(Struct(  # Directory entry map
                "len_len" / Default(IntInASCII(Bytes(1)), DEFAULT_LEN_LEN),
                "pos_len" / Default(IntInASCII(Bytes(1)), DEFAULT_POS_LEN),
                "custom_len" / Default(IntInASCII(Bytes(1)),
                                       DEFAULT_CUSTOM_LEN),
                "reserved" / Default(Bytes(1), b"0"),
            )),
        )),
        "num_fields" / Computed(
            (this.base_addr - LABEL_LEN - ft_len) //
            (TAG_LEN + this.len_len + this.pos_len + this.custom_len)
        ),
        Check(lambda this:
            "fields" not in this or this.num_fields == len(this.fields)
        ),

        # Directory
        "dir" / Struct(
            "tag" / Bytes(TAG_LEN),
            "len" / Rebuild(IntInASCII(Bytes(this._.len_len)),
                            lambda this: this._._build_len_list[this._index]),
            "pos" / Rebuild(IntInASCII(Bytes(this._.pos_len)),
                            lambda this: this._._build_pos_list[this._index]),
            "custom" / Default(Bytes(this._.custom_len),
                               b"0" * this._.custom_len),
        )[this.num_fields],
        Check(lambda this: this.num_fields == 0 or (
            this.dir[0].pos == 0 and
            all(
                this.dir[idx + 1].pos == entry.pos + entry.len
                for idx, entry in enumerate(this.dir[:-1])
            )
        )),
        Const(field_terminator),

        # Field data
        "fields" / Array(
            this.num_fields,
            CheckTrimSuffix(
                Bytes(lambda this: this.dir[this._index].len),
                field_terminator,
            ),
        ),

        # There should be no more data belonging to this record
        Const(record_terminator),
    )


DEFAULT_RECORD_STRUCT = LineSplitRestreamed(create_record_struct())


@should_be_file("iso_file")
def iter_con(iso_file, record_struct=DEFAULT_RECORD_STRUCT):
    """Generator of records as parsed construct objects."""
    alt_struct = Select(record_struct, Terminated)
    while True:
        con = alt_struct.parse_stream(iso_file)
        if con is None:  # No more records
            return
        yield con


def iter_records(iso_file, encoding=DEFAULT_ISO_ENCODING, **kwargs):
    """Generator of records as dictionaries."""
    for con in iter_con(iso_file, **kwargs):
        yield con2dict(con, encoding=encoding)


def con2dict(con, encoding=DEFAULT_ISO_ENCODING):
    """Parsed construct object to dictionary record converter."""
    result = defaultdict(list)
    for dir_entry, field_value in zip(con.dir, con.fields):
        tag = dir_entry.tag.lstrip(b"0").decode("ascii") or b"0"
        result[tag].append(field_value.decode(encoding))
    return result


def dict2bytes(
    data,
    encoding=DEFAULT_ISO_ENCODING,
    record_struct=DEFAULT_RECORD_STRUCT,
):
    """Encode/build the raw ISO string from a single dict record."""
    record_dict = {
        "dir": [],
        "fields": [],
    }
    for k, values in data.items():
        for v in values:
            record_dict["dir"].append({"tag": k.encode("ascii").zfill(3)})
            record_dict["fields"].append(v.encode(encoding))
    return record_struct.build(record_dict)
