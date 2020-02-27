from collections import Counter, defaultdict
from itertools import cycle, zip_longest
import re


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


def tl2record(tl, sfp=None, mode="field"):
    """Converter of a record from a tidy list to a dictionary."""
    if mode in "field":
        items = tl
    elif mode == "pairs":
        items = [(k, sfp(v)) for k, v in tl]
    elif mode == "nest":
        items = [(k, dict(sfp(v))) for k, v in tl]
    else:
        raise ValueError(f"Unknown mode {mode!r}")
    result = defaultdict(list)
    for tag, field in items:
        result[tag].append(field)
    return result


def record2tl(record, sfp=None, mode="field"):
    items = []
    for k, values in record.items():
        for v in values:
            items.append((k, v))

    if mode in "field":
        return items
    elif mode == "pairs":
        return [(k, sfp.unparse(*v)) for k, v in items]
    elif mode == "nest":
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
    return [nest_decode(value, encoding) for value in obj]


def nest_encode(obj, encoding):
    """Encode records in dict or tidy list format."""
    if hasattr(obj, "encode"):  # isinstance(obj, str)
        return obj.encode(encoding)
    if hasattr(obj, "items"):  # isinstance(obj, dict)
        return {k.encode("ascii"): nest_encode(v, encoding)
                for k, v in obj.items()}
    return [nest_encode(value, encoding) for value in obj]


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
    return [utf8_fix_nest_decode(value, encoding) for value in obj]


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
