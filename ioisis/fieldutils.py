from collections import Counter, defaultdict
import re


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
                 empty=False, number=True, zero=False):
        self.lower = lower
        self.empty = empty
        self.number = number
        self.zero = zero

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
        else:
            self.first = first

    def __call__(self, field):
        """Generate (key, value) pairs for each subfields in a field."""
        key_count = Counter()
        for key, value in self.subfields_regex.findall(field):
            if self.empty or value:
                if not key:  # PyPy: empty key is always str, not bytes
                    key = self.first
                if self.lower:
                    key = key.lower()
                if self.number:
                    suffix_int = key_count[key]
                    key_count[key] += 1
                    if self.zero or suffix_int:
                        key += self.percent_d % suffix_int
                yield key, value


def tl2dict(tl):
    """Converter of a record from a tidy list to a dictionary."""
    result = defaultdict(list)
    for tag, field in tl:
        result[tag].append(field)
    return result


def tl_decode(obj, encoding):
    if hasattr(obj, "decode"):  # isinstance(obj, bytes)
        return obj.decode(encoding)
    if hasattr(obj, "items"):  # isinstance(obj, dict)
        return {k.decode("ascii"): tl_decode(v, encoding)
                for k, v in obj.items()}
    return [tl_decode(value, encoding) for value in obj]
