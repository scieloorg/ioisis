from collections import Counter, defaultdict
from itertools import cycle, groupby, zip_longest
import re


DEFAULT_FTF_TEMPLATE = b"%z"

# The UTF-8 bytes (and number of bits to store a code point) are:
#
# - 0b0xxx_xxxx: single-byte sequence (ASCII)
# - 0b10xx_xxxx: continuation byte
# - 0b110x_xxxx: start byte of a 2-bytes sequence (11 CP bits)
# - 0b1110_xxxx: start byte of a 3-bytes sequence (16 CP bits)
# - 0b1111_0xxx: start byte of a 4-bytes sequence (21 CP bits)
#
# Code points of ASCII bytes should not be "overlong" UTF-8 sequences:
# 2-bytes sequences can encode unicode up to 11 code point bits,
# but we need to enforce it to have at least 8 code point bits,
# so the lowest 2-bytes valid sequence in UTF-8 is 0xc280 (U+0080).
# Likewise, the last unicode code point is U+10ffff,
# and a sequence should not represent a code point greater than that,
# so the highest 4-bytes UTF-8 is 0xf48fbfbf.
# Sequences should be stored with as fewer bytes as possible,
# so the smallest 3-bytes UTF-8 is 0xe0a080 (U+0800)
# and the smallest 4-bytes UTF-8 is 0xf0908080 (U+010000).
#
# Apart from that, the code points in the U+D800 to U+DFFF range
# aren't UTF-8 valid because they had been reserved
# for encoding data as surrogate pairs in UTF-16.
# It means that the range from 0xeda080 to 0xedbfbf is invalid.
#
# See also RFC3629 (https://tools.ietf.org/html/rfc3629)
# for an authoritative source of the information above.
UTF8_MB_REGEX = re.compile(  # Gets joined UTF-8 multi-byte sequences
    b"((?:[\\xc2-\\xdf][\\x80-\\xbf]"
    b" |   \\xe0       [\\xa0-\\xbf][\\x80-\\xbf]"
    b" |  [\\xe1-\\xec][\\x80-\\xbf][\\x80-\\xbf]"
    b" |   \\xed       [\\x80-\\x9f][\\x80-\\xbf]"
    b" |  [\\xee-\\xef][\\x80-\\xbf][\\x80-\\xbf]"
    b" |   \\xf0       [\\x90-\\xbf][\\x80-\\xbf][\\x80-\\xbf]"
    b" |  [\\xf1-\\xf3][\\x80-\\xbf][\\x80-\\xbf][\\x80-\\xbf]"
    b" |   \\xf4       [\\x80-\\x8f][\\x80-\\xbf][\\x80-\\xbf]"
    b" )+)",
    re.VERBOSE,
)

_EMPTY = object()


def _int_scanf_regex_str(size=1, zero=False):
    if zero:
        return f"(\\d{{{size},}}|-\\d{{{max(size - 1, 1)},}})"
    return "(" + "|".join(
        " " * (size - idx - 1) + "[0-9-]" + r"\d" * idx for idx in range(size)
    ) + r"\d*)"


class FieldTagFormatter:
    """Format all tags in the given tidy list.
    The format string (template) can include ``%%``:

    - ``%d'': Tag number where the leading zeros or whitespaces
              might be controled with a numeric parameter
              between these two characters, like the printf ``%d''.
    - ``%r'': Tag as a string in its raw format,
              either a 3-character string (ISO)
              or a number without leading zeros (MST).
              The same behavior of ``%d'' without parameters.
    - ``%z'': Same to ``%r'', but always removes the leading zeros,
              even from an ISO tag that isn't numeric.
    - ``%i'': Field index number in the record,
              it accepts a numeric parameter like ``%d''.
    - ``%%'': Escape the ``%'' character.

    Parameters
    ----------
    template : str or bytes
        Format string to build/parse the tag string
        from the raw tag and index.
    int_tags : bool
        If True, the raw tags are always integers (MST).
        If False, the raw tags are strings with 3 characters (ISO).
    """
    def __init__(self, template, int_tags):
        self.template = template
        self.int_tags = int_tags
        self.is_bytes = isinstance(template, bytes)

        # Stuff to enforce the data type (str or bytes),
        # cached in a single custom dict
        sfix = {
            False: lambda text: text,
            True: lambda text: text.encode("ascii"),
        }[self.is_bytes]
        DictFix = type("DictFix", (dict,), {"__missing__": staticmethod(sfix)})
        DictFix.__getattr__ = DictFix.__getitem__
        self._df = df = DictFix()  # Access by item/attribute its name

        # The template parser core
        regex_str = df[r"%(?P<size>0?[1-9]\d*)?(?P<code>[^%]|$)|(?:[^%]|%%)+"]

        # Builds from the template (including its data type)
        # a format string to use with the "%" operator
        # and a regex to parse a rendered tag string like a "scanf"
        self.need_rtag = self.need_ztag = self.need_itag = False
        parts = []
        sparts = []
        self.sparams = []
        for match in re.finditer(regex_str, template):
            gtext = match.group()
            gdict = match.groupdict()
            gcode = gdict["code"]
            gsize = gdict["size"] or df[""]
            gsize_int = int(gsize) if gsize else 1
            gzero = gsize.startswith(df["0"]) or not gsize
            if gcode is None:  # The regex makes sure there's no size
                parts.append(gtext)
                sparts.append(re.escape(gtext.replace(df["%%"], df["%"])))
            elif gtext == df["%r"]:
                parts.append(df["%(rtag)s"])
                self.need_rtag = True
                sparts.append(df[r"(\d+)" if int_tags else r"(.{1,3})"])
                self.sparams.append(("tag", None))
            elif gtext == df["%z"]:
                parts.append(df["%(ztag)s"])
                self.need_ztag = True
                sparts.append(df[r"([1-9]\d*|0)" if int_tags else
                                 r"([^0]..|[^0].|.)"])
                self.sparams.append(("tag", None))
            elif gcode == df["d"]:  # %d (itag)
                parts.extend([df["%(itag)"], gsize, df["d"]])
                self.need_itag = True
                sparts.append(df[_int_scanf_regex_str(gsize_int, gzero)])
                self.sparams.append(("tag", df["0"] if gzero else df[" "]))
            elif gcode == df["i"]:  # %i (index)
                parts.extend([df["%(index)"], gsize, df["d"]])
                sparts.append(df[_int_scanf_regex_str(gsize_int, gzero)])
                self.sparams.append(("index", df["0"] if gzero else df[" "]))
            else:
                raise ValueError(f"Unknown format {gtext!r}")
        self._fmt = df[""].join(parts)
        self._scanf_regex = re.compile(df[""].join(sparts))

    def __call__(self, tag, index=-1):
        """Convert the given tag, itag and index (keyword arguments)
        to a formatted tag string.
        The required arguments are the ones that appears
        in the format string of this instance.
        """
        df = self._df
        is_int = self.int_tags
        kwargs = {df.index: index}
        if self.need_rtag:
            kwargs[df.rtag] = (df["%d"] % tag) if is_int else tag
        if self.need_itag:
            kwargs[df.itag] = tag if is_int else int(tag, base=10)
        if self.need_ztag:
            if is_int:
                kwargs[df.ztag] = df["%d"] % tag
            else:
                zero = df["0"]
                kwargs[df.ztag] = tag.lstrip(zero) or zero
        return self._fmt % kwargs

    def scanf(self, value):
        """Get the tag string and index from its rendered value."""
        match = self._scanf_regex.match(value)
        if not match:
            raise ValueError(f"Invalid field tag string {value!r}")

        result = defaultdict(list)
        for (key, prefix), part in zip(self.sparams, match.groups()):
            result[key].append(
                part if prefix is None else
                (part.lstrip(prefix) or self._df["0"])
            )

        if "tag" not in result:
            tags = {None}
        elif self.int_tags:
            tags = set(map(int, result["tag"]))
        else:
            tags = set(t.zfill(3) for t in result["tag"])
        if len(tags) != 1:
            raise ValueError(f"Multiple tag in field tag string {value!r}")

        indexes = set(map(int, result.get("index", [-1])))
        if len(indexes) != 1:
            raise ValueError(f"Multiple index in field tag string {value!r}")

        return tags.pop(), indexes.pop()


def con_pairs(con, ftf):
    """Generator of raw ``(tag, field)`` pairs of ``bytes`` objects.
    The input should be a raw construct container (dictionary)
    representing a single record from a parsed ISO or MST file.
    """
    for idx, (dir_entry, field_value) in enumerate(zip(con.dir, con.fields)):
        yield ftf(dir_entry.tag, idx), field_value


def tl2con(tl, ftf):
    """Create a record dict that can be used for ISO/MST building
    from a single tidy list record."""
    container = {
        "dir": [],
        "fields": [],
    }
    for k, v in tl:
        if k == b"mfn":
            container["mfn"] = int(v)  # Makes no difference for ISO
        else:
            tag, index = ftf.scanf(k)
            container["dir"].append({"tag": tag})
            container["fields"].append(v)
    return container


class SubfieldParser:
    """Generate subfield pairs from the given value on calling.

    Parameters
    ----------
    prefix : str or bytes
        Marker of the beginning of a new subfield.
        The type of this prefix should be the same
        of the fields to be parsed.
    length : int
        Subfield key length after its mark in the field.
    lower : bool
        Force keys to be in lowercase, making them case insensitive.
    first : str, bytes or None
        The key to be used for the leading value.
    empty : bool
        Keep pairs whose value is empty.
    number : bool
        Add a number suffix in all but the first
        of each distinct subfield key,
        grouping this indexing by the key
        and starting the numbering an implicit ``0``,
        so the subfield with a key that appeared for the second time
        should have a ``1'' suffix.
    zero : bool
        Also append the ``0`` in subfields keys,
        so that all keys should have a suffix
        (including the first/leading "keyless" pair).
        Has no effect if ``number`` is ``False``.
    check : bool
        Check data consistency on unparsing.
        See ``SubfieldParser.unparse`` for more information.

    Examples
    --------
    It can be used to convert the field to a dict:

    >>> gen_subfields = SubfieldParser(b"^", first=b"_")
    >>> dict(gen_subfields(b"data^ttext^len^tTrail"))
    {b'_': b'data', b't': b'text', b'l': b'en', b't1': b'Trail'}

    Or to a list, and the type (bytes/str) is defined by the prefix:

    >>> str_subfields = SubfieldParser("#F#", number=False)
    >>> list(str_subfields("data#F#ttext#F#len#F#tTrail"))
    [('', 'data'), ('t', 'text'), ('l', 'en'), ('t', 'Trail')]

    """
    def __init__(self, prefix, *, length=1, lower=False, first=None,
                 empty=False, number=True, zero=False, check=True):
        self.prefix = prefix
        self.length = length
        self.lower = lower
        self.empty = empty
        self.number = number
        self.zero = zero
        self.check = check

        escaped_prefix = re.escape(prefix)
        regex_str = b"(?:^|(?<=%s(.{%d})))((?:(?!%s.{%d}).)*)"
        if isinstance(prefix, str):
            regex_str = regex_str.decode("ascii")
            self.percent_d = "%d"
        else:
            self.percent_d = b"%d"
        regex_str %= (escaped_prefix, length) * 2
        self.subfields_regex = re.compile(regex_str, re.DOTALL)

        if first is None:
            self.first = regex_str[:0]  # Empty bytes or str
        elif lower:
            self.first = first.lower()
        else:
            self.first = first

        if self.number and self.zero:
            self.fz = self.first + (self.percent_d % 0)
        else:
            self.fz = self.first

    def __call__(self, field):
        """Generate (key, value) pairs for each subfields in a field."""
        key_count = Counter()
        for key, value in self.subfields_regex.findall(field):
            if self.empty or value:
                if not key:  # PyPy: empty key is always str, not bytes
                    key = self.first
                elif self.lower:
                    key = key.lower()
                if self.number:
                    suffix_int = key_count[key]
                    key_count[key] += 1
                    if self.zero or suffix_int:
                        key += self.percent_d % suffix_int
                yield key, value

    def unparse(self, *subfields, check=_EMPTY):
        """Build the field from the ordered subfield pairs.

        Parameters
        ----------
        *subfields
            Subfields as ``(key, value)`` pairs (tuples)
            of ``bytes`` or str``.
        check : bool
            Force checking if this SubfieldParser
            would generate exactly the same subfields from the result.
            That won't happen if the input can't be created
            by this SubfieldParser instance,
            so it's a way to check the number suffixes,
            empty subfields, subfields keys in upper case,
            and invalid contents like a subfield inside another.
        """
        blocks = []
        if subfields and ((subfields[0][0] == self.fz) or
                          (self.lower and subfields[0][0].lower() == self.fz)):
            blocks.append(subfields[0][1])
            remaining = subfields[1:]
        else:
            remaining = subfields

        for key, value in remaining:
            if self.empty or value:
                if len(key) < self.length:
                    raise ValueError(f"Incomplete key data {key!r}")
                if self.lower:
                    key = key.lower()
                blocks.append(self.prefix + key[:self.length] + value)

        result = self.prefix[:0].join(blocks)
        has_to_check = self.check if check is _EMPTY else check
        if has_to_check:
            self._parse_check(result, *subfields)
        return result

    def _parse_check(self, field, *subfields):
        """Check if the subfields are the parsed field."""
        parsed = self(field)
        pairs_of_pairs = zip_longest(parsed, subfields, fillvalue=(None, None))
        for idx, ((kp, vp), (ks, vs)) in enumerate(pairs_of_pairs):
            if ks != kp:
                raise ValueError(f"Invalid subfield[{idx}] key {ks!r}")
            if vs != vp:
                raise ValueError(f"Invalid subfield[{idx}] value {vs!r}")


def inest(pairs):
    """Dict creation function
    that keeps the first value instead of the last one
    when a key is repeated.
    """
    result = {}
    for pair in pairs:
        result.setdefault(*pair)
    return result


def _tidy_tl2record(tl, sfp=None, split_sub=False):
    tlit = iter(tl)
    mfn_key, mfn = next(tlit)
    mfn = int(mfn)
    if mfn_key not in [b"mfn", "mfn"]:
        raise ValueError("Missing MFN")
    index, tag, data, sindex, sub = (
        ("index", "tag", "data", "sindex", "sub")
        if isinstance(mfn_key, str) else
        (b"index", b"tag", b"data", b"sindex", b"sub")
    )
    if split_sub:
        return [{mfn_key: mfn, index: idx, tag: k,
                 sindex: sidx, sub: sk, data: sv}
                for idx, (k, v) in enumerate(tlit)
                for sidx, (sk, sv) in enumerate(sfp(v))]
    return [{mfn_key: mfn, index: idx, tag: k, data: v}
            for idx, (k, v) in enumerate(tlit)]


def tl2record(tl, sfp=None, mode="field"):
    """Converter of a record from a tidy list of (key, value) pairs
    to either a dictionary (field/pairs/nest/inest modes)
    or a tidy list of dictionaries (tidy/stidy mode).
    """
    if mode in ["tidy", "stidy"]:  # Requires --prepend-mfn
        return _tidy_tl2record(tl, sfp=sfp, split_sub=(mode == "stidy"))

    if mode == "field":
        items = tl
    elif mode == "pairs":
        items = [(k, sfp(v)) for k, v in tl]
    elif mode == "nest":
        items = [(k, dict(sfp(v))) for k, v in tl]
    elif mode == "inest":
        items = [(k, inest(sfp(v))) for k, v in tl]
    else:
        raise ValueError(f"Unknown mode {mode!r}")
    result = defaultdict(list)
    for tag, field in items:
        result[tag].append(field)
    return result


def stidy2tidy(record, sfp=None):
    mfn_key, index, tag, data, percent_d, sindex, sub = (
        ("mfn", "index", "tag", "data", "%d", "sindex", "sub")
        if isinstance(next(iter(record[0].keys())), str) else
        (b"mfn", b"index", b"tag", b"data", b"%d", b"sindex", b"sub")
    )
    fields = []
    for unused, grp in groupby(
        record,
        key=lambda field: (field[index], field[mfn_key], field[tag]),
    ):
        subfields = list(grp)
        for sidx, subfield_dict in enumerate(subfields):
            if sidx != subfield_dict[sindex]:
                raise ValueError("Invalid sindex numbering")
        pairs = [(subfield_dict[sub], subfield_dict[data])
                 for subfield_dict in subfields]
        first = subfields[0]
        fields.append({
            mfn_key: first[mfn_key],
            index: first[index],
            tag: first[tag],
            data: sfp.unparse(*pairs),
        })
    return fields


def _tidy_record2tl(record, sfp=None, split_sub=False, prepend_mfn=False):
    if split_sub:
        record = stidy2tidy(record, sfp=sfp)
    mfn_key, index, tag, data, percent_d = (
        ("mfn", "index", "tag", "data", "%d")
        if isinstance(next(iter(record[0].keys())), str) else
        (b"mfn", b"index", b"tag", b"data", b"%d")
    )
    mfn = record[0][mfn_key]
    items = []
    if prepend_mfn:
        items.append((mfn_key, percent_d % mfn))
    for idx, field_dict in enumerate(record):
        if mfn != field_dict[mfn_key]:  # Should never happen from the CLI
            raise ValueError("Multiple MFN in a single record")
        if idx != field_dict[index]:
            raise ValueError("Invalid index numbering")
        items.append((field_dict[tag], field_dict[data]))
    return items


def record2tl(record, sfp=None, mode="field", prepend_mfn=False):
    if mode in ["tidy", "stidy"]:  # Tidy list of dictionaries
        return _tidy_record2tl(record, sfp=sfp, split_sub=(mode == "stidy"),
                               prepend_mfn=prepend_mfn)

    items = []
    for k, values in record.items():
        for v in values:
            items.append((k, v))

    if mode == "field":
        return items
    elif mode == "pairs":
        return [(k, sfp.unparse(*v)) for k, v in items]
    elif mode in ["nest", "inest"]:
        return [(k, sfp.unparse(*v.items())) for k, v in items]
    else:
        raise ValueError(f"Unknown mode {mode!r}")


def nest_decode(obj, encoding):
    """Decode records in dict or tidy list format."""
    if hasattr(obj, "decode"):  # isinstance(obj, bytes)
        return obj.decode(encoding)
    if hasattr(obj, "items"):  # isinstance(obj, dict)
        return {k.decode("ascii"): nest_decode(v, encoding)
                for k, v in obj.items()}
    if hasattr(obj, "__iter__"):
        return [nest_decode(value, encoding) for value in obj]
    return obj


def nest_encode(obj, encoding):
    """Encode records in dict or tidy list format."""
    if hasattr(obj, "encode"):  # isinstance(obj, str)
        return obj.encode(encoding)
    if hasattr(obj, "items"):  # isinstance(obj, dict)
        return {k.encode("ascii"): nest_encode(v, encoding)
                for k, v in obj.items()}
    if hasattr(obj, "__iter__"):
        return [nest_encode(value, encoding) for value in obj]
    return obj


def utf8_fix_nest_decode(obj, encoding):
    """Decode records in dict or tidy list format
    using an hybrid strategy where a UTF-8 decoding
    is tried before the given encoding.
    """
    if hasattr(obj, "decode"):  # isinstance(obj, bytes)
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            return hybrid_utf8_decode(obj, encoding)
    if hasattr(obj, "items"):  # isinstance(obj, dict)
        return {k.decode("ascii"): utf8_fix_nest_decode(v, encoding)
                for k, v in obj.items()}
    if hasattr(obj, "__iter__"):
        return [utf8_fix_nest_decode(value, encoding) for value in obj]
    return obj


def hybrid_utf8_decode(value, encoding):
    """Decode a bytestring value that might be partially in UTF-8,
    partially in the other given encoding,
    trying to do that with UTF-8 before."""
    return "".join(
        seq.decode(enc)
        for seq, enc in zip(UTF8_MB_REGEX.split(value),
                            cycle([encoding, "utf-8"]))
        if seq
    )
