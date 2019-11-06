"""Model for the ISIS ISO2709-based file format.

This file format specification can be found at:

https://wiki.bireme.org/pt/img_auth.php/5/5f/2709BR.pdf
"""
from collections import defaultdict
from functools import partial
import re

from construct import Adapter, Array, Bytes, Check, CheckError, Computed, \
                      Const, Default, Embedded, Rebuild, Restreamed, \
                      Struct, this


FIELD_TERMINATOR = b"#"
RECORD_TERMINATOR = b"#"


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


single_record_struct = Restreamed(
    Struct(
        # Record label/header
        Embedded(Struct(
            "total_len" / IntInASCII(Bytes(5)),
            "status" / Default(Bytes(1), b"0"),
            "type" / Default(Bytes(1), b"0"),
            "custom_2" / Default(Bytes(2), b"00"),
            "coding" / Default(Bytes(1), b"0"),
            "indicator_count" / Default(IntInASCII(Bytes(1)), 0),
            "identifier_len" / Default(IntInASCII(Bytes(1)), 0),
            "base_addr" / IntInASCII(Bytes(5)),
            "custom_3" / Default(Bytes(3), b"000"),
            Embedded(Struct(  # Directory entry map
                "len_len" / Default(IntInASCII(Bytes(1)), 4),
                "pos_len" / Default(IntInASCII(Bytes(1)), 5),
                "custom_len" / Default(IntInASCII(Bytes(1)), 0),
                "reserved" / Default(Bytes(1), b"0"),
            )),
        )),
        "num_fields" / Computed((this.base_addr - 25) // 12),

        # Directory
        "dir" / Struct(
            "tag" / Bytes(3),
            "len" / IntInASCII(Bytes(this._.len_len)),
            "pos" / IntInASCII(Bytes(this._.pos_len)),
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
        Const(FIELD_TERMINATOR),

        # Field data
        "fields" / Array(
            this.num_fields,
            CheckTrimSuffix(
                Bytes(lambda this: this.dir[this._index].len),
                FIELD_TERMINATOR,
            ),
        ),

        # There should be no more data belonging to this record
        Const(RECORD_TERMINATOR),
    ),
    decoder=partial(re.compile(b"[\r\n]").sub, b""),
    decoderunit=1,
    encoder=lambda chunk: chunk,
    encoderunit=1,
    sizecomputer=lambda n: n,
)


def con2dict(con, encoding="cp1252"):
    """Parsed construct object to dictionary record converter."""
    result = defaultdict(list)
    for dir_entry, field_value in zip(con.dir, con.fields):
        tag = dir_entry.tag.lstrip(b"0").decode("ascii") or b"0"
        result[tag].append(field_value.decode(encoding))
    return result
