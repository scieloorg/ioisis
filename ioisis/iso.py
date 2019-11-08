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
                      Const, Default, Embedded, Rebuild, Restreamed, \
                      RestreamedBytesIO, Struct, this

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


class RestreamedBytesIOWriteLastIncomplete(RestreamedBytesIO):
    """Alternative to RestreamedBytesIO
    that flushes the output buffer on building before closing.
    """
    def close(self):
        if self.wbuffer:
            self.substream.write(self.encoder(self.wbuffer))
            self.wbuffer = b""
        super(RestreamedBytesIOWriteLastIncomplete, self).close()


class RestreamedBuildLastIncomplete(Restreamed):
    """Alternative to Restreamed
    that uses RestreamedBytesIOWriteLastIncomplete
    instead of RestreamedBytesIO.
    The difference is that this class
    encodes the last incomplete chunk on building
    instead of aborting the building process.
    """
    def _build(self, obj, stream, context, path):
        with closing(RestreamedBytesIOWriteLastIncomplete(
            substream=stream,
            decoder=self.decoder,
            decoderunit=self.decoderunit,
            encoder=self.encoder,
            encoderunit=self.encoderunit,
        )) as stream2:
            self.subcon._build(obj, stream2, context, path)
        return obj


def line_split_restreamed(
    subcon,
    line_len=DEFAULT_LINE_LEN,
    newline=DEFAULT_NEWLINE
):
    """Decorates a subconstruct object
    with something like ``construct.Restreamed``
    to parse/build the contents in a text-like structure,
    neglecting the CR and LF separators on parsing
    and using the chosen newline separator on building
    for the given line length.
    On parsing, this doesn't check the stream splitting format,
    it just discards the CR/LF no matter where they are.
    """
    size_extra = len(newline)
    return RestreamedBuildLastIncomplete(
        subcon,
        decoder=clear_cr_lf,
        decoderunit=1,
        encoder=lambda chunk: chunk + newline,
        encoderunit=line_len,
        sizecomputer=lambda n: n + (n // line_len + 1) * size_extra,
    )


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
            "custom" / Rebuild(Bytes(this._.custom_len),
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


DEFAULT_RECORD_STRUCT = line_split_restreamed(create_record_struct())


@should_be_file("iso_file")
def iter_con(iso_file, record_struct=DEFAULT_RECORD_STRUCT):
    """Generator of records as parsed construct objects."""
    while clear_cr_lf(iso_file.peek(5)):  # Has another record
        yield record_struct.parse_stream(iso_file)


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
