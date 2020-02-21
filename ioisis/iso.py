"""Model for the ISIS ISO2709-based file format.

This file format specification can be found at:

https://wiki.bireme.org/pt/img_auth.php/5/5f/2709BR.pdf
"""
from collections import defaultdict
from itertools import accumulate

from construct import Array, Bytes, Check, Computed, \
                      Const, Container, Default, ExprAdapter, \
                      FocusedSeq, Prefixed, RawCopy, \
                      Rebuild, Select, Struct, Terminated, this

from .ccons import IntInASCII, LineSplitRestreamed, \
                   DEFAULT_LINE_LEN, DEFAULT_NEWLINE
from .streamutils import should_be_file, TightBufferReadOnlyBytesStreamWrapper


DEFAULT_FIELD_TERMINATOR = b"#"
DEFAULT_RECORD_TERMINATOR = b"#"
DEFAULT_ISO_ENCODING = "cp1252"

TOTAL_LEN_LEN = 5
LEADER_LEN = TOTAL_LEN_LEN + 19
TAG_LEN = 3
DEFAULT_LEN_LEN = 4
DEFAULT_POS_LEN = 5
DEFAULT_CUSTOM_LEN = 0


def create_record_struct(
    field_terminator=DEFAULT_FIELD_TERMINATOR,
    record_terminator=DEFAULT_RECORD_TERMINATOR,
    line_len=DEFAULT_LINE_LEN,
    newline=DEFAULT_NEWLINE,
):
    """Create a construct parser/builder for a whole record object."""
    ft_len = len(field_terminator)
    prefixless = Struct(
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

        # Record leader/header (apart from the leading total_len)
        "status" / Default(Bytes(1), b"0"),
        "type" / Default(Bytes(1), b"0"),
        "custom_2" / Default(Bytes(2), b"00"),
        "coding" / Default(Bytes(1), b"0"),
        "indicator_count" / Default(IntInASCII(Bytes(1)), 0),
        "identifier_len" / Default(IntInASCII(Bytes(1)), 0),
        "base_addr" / Rebuild(IntInASCII(Bytes(5)),
                              LEADER_LEN + this._build_dir_len
                                         + ft_len),
        "custom_3" / Default(Bytes(3), b"000"),

        # Directory entry map (trailing part of the leader)
        "len_len" / Default(IntInASCII(Bytes(1)), DEFAULT_LEN_LEN),
        "pos_len" / Default(IntInASCII(Bytes(1)), DEFAULT_POS_LEN),
        "custom_len" / Default(IntInASCII(Bytes(1)),
                               DEFAULT_CUSTOM_LEN),
        "reserved" / Default(Bytes(1), b"0"),

        # The ISO leader/header doesn't have the number of fields,
        # but it can be found from the base address
        "num_fields" / Computed(
            (this.base_addr - LEADER_LEN - ft_len) //
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
        Check(lambda this: this._io.tell() + TOTAL_LEN_LEN == this.base_addr),

        # Field data
        "fields" / Array(
            this.num_fields,
            FocusedSeq(
                "value",
                "value" / Bytes(
                    lambda this: this._.dir[this._index].len - ft_len
                ),
                Const(field_terminator),
            ),
        ),

        # There should be no more data belonging to this record
        Const(record_terminator),
    )

    # This includes (and checks) the total_len prefix
    result = ExprAdapter(
        RawCopy(Prefixed(
            lengthfield=IntInASCII(Bytes(TOTAL_LEN_LEN)),
            subcon=prefixless,
            includelength=True,
        )),
        lambda obj, ctx: Container(total_len=obj.length, **obj.value),
        lambda obj, ctx: {"value": obj},
    )

    if line_len is None or line_len == 0:
        return result
    return LineSplitRestreamed(result, line_len=line_len, newline=newline)


DEFAULT_RECORD_STRUCT = create_record_struct()


@should_be_file("iso_file")
def iter_con(iso_file, record_struct=DEFAULT_RECORD_STRUCT):
    """Generator of records as parsed construct objects."""
    alt_struct = Select(record_struct, Terminated)
    while True:
        stream_reader = TightBufferReadOnlyBytesStreamWrapper(iso_file)
        con = alt_struct.parse_stream(stream_reader)
        if con is None:  # No more records
            return
        yield con


def iter_records(iso_file, encoding=DEFAULT_ISO_ENCODING, **kwargs):
    """Generator of records as dictionaries."""
    for con in iter_con(iso_file, **kwargs):
        yield con2dict(con, encoding=encoding)


def con_pairs(con):
    """Generator of raw ``(tag, field)`` pairs of ``bytes`` objects."""
    for dir_entry, field_value in zip(con.dir, con.fields):
        yield dir_entry.tag.lstrip(b"0") or b"0", field_value


def iter_raw_tl(iso_file, *, only_active=True, prepend_status=False,
                record_struct=DEFAULT_RECORD_STRUCT):
    for con in iter_con(iso_file, record_struct=record_struct):
        if only_active and con.status != b"0":
            continue
        result = []
        if prepend_status:
            result.append((b"status", b"%d" % con.status))
        result.extend(con_pairs(con))
        yield result


def iter_tl(iso_file, encoding=DEFAULT_ISO_ENCODING, **kwargs):
    for tl in iter_raw_tl(iso_file, **kwargs):
        yield [(tag.decode("ascii"), field.decode(encoding))
               for tag, field in tl]


def con2dict(con, encoding=DEFAULT_ISO_ENCODING):
    """Parsed construct object to dictionary record converter."""
    result = defaultdict(list)
    for tag_value, field_value in con_pairs(con):
        result[tag_value.decode("ascii")].append(field_value.decode(encoding))
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


def tl2bytes(tl, record_struct=DEFAULT_RECORD_STRUCT):
    """Encode/build the raw ISO string from a single tidy list record."""
    container = {
        "dir": [],
        "fields": [],
    }
    for k, v in tl:
        container["dir"].append({"tag": k.zfill(3)})
        container["fields"].append(v)
    return record_struct.build(container)
