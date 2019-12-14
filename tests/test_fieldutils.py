import pytest

from ioisis.fieldutils import SubfieldParser


SFP_CALL_TEST_TABLE = {  # Items are {id: (field, expected, kwargs)}
    # Empty input
    "empty_false":
        ("", [], dict(prefix="x")),
    "empty_true":
        ("", [("", "")], dict(prefix="x", empty=True)),

    # Single non-empty subfield (and perhaps some empty subfield)
    "single_nonempty_subfield_first":
        ("data", [("", "data")], dict(prefix="^")),
    "single_nonempty_subfield_no_first":
        ("data", [("a", "ta")], dict(prefix="d")),
    "single_nonempty_subfield_empty_first":
        ("data", [("", ""), ("a", "ta")], dict(prefix="d", empty=True)),

    # Non-subfield prefix (trailing prefix and field named with prefix)
    "non_subfield_prefix":
        ("data", [("", "d"), ("t", "a")], dict(prefix="a")),

    # UTF-8 / multi-byte prefix
    "utf8_prefix":
        ("dátá", [("", "d"), ("t", "á")], dict(prefix="á")),
    "multibyte_ascii_prefix":
        ("#-#ak0#-ak-#", [("#", "ak0"), ("a", "k-#")], dict(prefix="#-")),

    # Length, number and zero
    "length_2_ignore_empty":
        ("data", [("", "d")], dict(prefix="a", length=2)),
    "length_2_keep_empty": (
        "data",
        [("", "d"), ("ta", "")],
        dict(prefix="a", length=2, empty=True),
    ),
    "length_0_ignore_empty":
        ("data", [("", "d"), ("1", "t")], dict(prefix="a", length=0)),
    "length_0_ignore_empty_no_number": (
        "data",
        [("", "d"), ("", "t")],
        dict(prefix="a", length=0, number=False),
    ),
    "length_0_keep_empty": (
        "data",
        [("", "d"), ("1", "t"), ("2", "")],
        dict(prefix="a", length=0, empty=True),
    ),
    "length_0_keep_empty_no_number": (
        "ðata",
        [("", "ð"), ("", "t"), ("", "")],
        dict(prefix="a", length=0, empty=True, number=False),
    ),
    "length_0_keep_empty_zero": (
        "data",
        [("0", "d"), ("1", "t"), ("2", "")],
        dict(prefix="a", length=0, empty=True, zero=True),
    ),

    # First, number and zero
    "first_unused":
        ("ioisis test", [("s", " test")], dict(prefix="i", first="1")),
    "first_empty": (
        "ioisis test",
        [("1", ""), ("o", ""), ("s", ""), ("s1", " test")],
        dict(prefix="i", first="1", empty=True),
    ),
    "first_empty_no_number": (
        "ioisis test",
        [("1", ""), ("o", ""), ("s", ""), ("s", " test")],
        dict(prefix="i", first="1", empty=True, number=False),
    ),
    "first_empty_zero": (
        "ioisis test",
        [("_0", ""), ("o0", ""), ("s0", ""), ("s1", " test")],
        dict(prefix="i", first="_", empty=True, zero=True),
    ),
    "first_with_3_bytes": (
        "ioisis test",
        [("1st", "io"), ("i", "s test")],
        dict(prefix="is", first="1st"),
    ),
    "first_with_3_bytes_and_remaining_with_length_2": (
        "ioisis test",
        [("1st", "io"), ("is", " test")],
        dict(prefix="is", first="1st", length=2),
    ),
    "first_with_3_bytes_and_remaining_with_length_2_number": (
        "ioisis test isis numbered",
        [("1st", "io"), ("is", " test "), ("is1", " numbered")],
        dict(prefix="is", first="1st", length=2),
    ),
    "first_with_3_bytes_and_remaining_with_length_2_number_zero": (
        "ioisis të§t isis numbered",
        [("1st0", "io"), ("is0", " të§t "), ("is1", " numbered")],
        dict(prefix="is", first="1st", length=2, zero=True),
    ),
    "first_with_3_bytes_and_remaining_with_length_2_no_number": (
        "ioisis test isisnt numbered",
        [("1st", "io"), ("is", " test "), ("is", "nt numbered")],
        dict(prefix="is", first="1st", length=2, number=False),
    ),

    # Lower
    "lower_no_number_length_2": (
        "7Asuiñ¼suidn7AIDjqoiw7siojAipoo7Aidosijd",
        [("su", "iñ¼suidn"), ("id", "jqoiw7siojAipoo"), ("id", "osijd")],
        dict(prefix="7A", length=2, lower=True, number=False),
    ),
    "number_no_lower_length_2": (
        "7Asuiñ¼suidn7AIDjqoiw7siojAipoo7Aidosijd",
        [("su", "iñ¼suidn"), ("ID", "jqoiw7siojAipoo"), ("id", "osijd")],
        dict(prefix="7A", length=2, lower=False, number=True),
    ),
    "lower_number_zero_length_2": (
        "7Asuiñ¼suidn7AIDjqoiw7siojAipoo7Aidosijd",
        [("su0", "iñ¼suidn"), ("id0", "jqoiw7siojAipoo"), ("id1", "osijd")],
        dict(prefix="7A", length=2, lower=True, number=True, zero=True),
    ),
    "lower_first_empty": (
        "",
        [("first", "")],
        dict(prefix="^", lower=True, first="FIRST", empty=True),
    ),
}


@pytest.mark.parametrize(
    "field, expected, kwargs",
    [pytest.param(*v, id=k) for k, v in SFP_CALL_TEST_TABLE.items()],
)
def test_sfp_call(field, expected, kwargs):
    # When empty=True it should be possible to resynthesize the field
    # (This first step is actually testing the test input)
    prefix = kwargs["prefix"]
    if expected:
        length = kwargs.get("length", 1)
        resynth = expected[0][1] + "".join(prefix + k[:length] + v
                                           for k, v in expected[1:])
        if kwargs.get("empty", False):
            assert resynth == field
        elif resynth == field:  # Then empty should make no difference
            test_sfp_call(field, expected, {**kwargs, "empty": True})

    # Test for the given [decoded] str parameters
    sfp_str = SubfieldParser(**kwargs)
    assert list(sfp_str(field)) == expected

    # Encode all str parameters and test with [encoded] bytes
    field_bytes = field.encode("utf-8")
    expected_bytes = [(k.encode("utf-8"), v.encode("utf-8"))
                      for k, v in expected]
    kwargs_bytes = {k: v.encode("utf-8") if isinstance(v, str) else v
                    for k, v in kwargs.items()}
    sfp_bytes = SubfieldParser(**kwargs_bytes)
    assert list(sfp_bytes(field_bytes)) == expected_bytes
