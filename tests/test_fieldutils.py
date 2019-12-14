from inspect import signature
import types

import pytest

from ioisis.fieldutils import SubfieldParser


SFP_SIGNATURE = signature(SubfieldParser)
SFP_DEFAULT_EMPTY = SFP_SIGNATURE.parameters["empty"].default
SFP_DEFAULT_LENGTH = SFP_SIGNATURE.parameters["length"].default
SFP_DEFAULT_LOWER = SFP_SIGNATURE.parameters["lower"].default

SFP_DATA = {  # Items are {id: (field, expected, kwargs)}
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
    "lower_empty_no_first_number": (
        "^ASOME^1DATA^AHERE",
        [("", ""), ("a", "SOME"), ("1", "DATA"), ("a1", "HERE")],
        dict(prefix="^", lower=True, empty=True, number=True),
    ),
    "lower_no_empty_not_resynthesizable": (
        "^ASOME^1^A^2^a^3DATA^a^AHERE^a¿",
        [("a", "SOME"), ("3", "DATA"), ("a1", "HERE"), ("a2", "¿")],
        dict(prefix="^", lower=True, empty=False, number=True),
    ),
}

# In SFP_DATA, either empty is False, lower is True or field == resynth
SFP_DATA_FIELD_RESYNTH_ASSUMING_EMPTY = {
    id_: expected[0][1] + "".join(
        kwargs["prefix"] + k[:kwargs.get("length", SFP_DEFAULT_LENGTH)] + v
        for k, v in expected[1:]
    )
    for id_, (field, expected, kwargs) in SFP_DATA.items()
    if expected
}
assert all(not kwargs.get("empty", SFP_DEFAULT_EMPTY)
           or kwargs.get("lower", SFP_DEFAULT_LOWER)
           or field == SFP_DATA_FIELD_RESYNTH_ASSUMING_EMPTY[id_]
           for id_, (field, expected, kwargs) in SFP_DATA.items())

# Copy of tests where empty=False but it can also be True
SFP_EXTRA_DATA = {
    id_ + "_with_empty_true": (field, expected, {**kwargs, "empty": True})
    for id_, (field, expected, kwargs) in SFP_DATA.items()
    if expected  # When empty=True, the result has at least one subfield
    and not kwargs.get("empty", SFP_DEFAULT_EMPTY)
    and field == SFP_DATA_FIELD_RESYNTH_ASSUMING_EMPTY[id_]
}
assert SFP_EXTRA_DATA  # We know there is at least one such case

# Build the test params to create the tests for both str and bytes
SFP_TEST_PARAMS_STR = [pytest.param(*v, id=k + "_decoded_str")
                       for k, v in {**SFP_DATA, **SFP_EXTRA_DATA}.items()]
SFP_TEST_PARAMS_BYTES = [
    pytest.param(
        field.encode("utf-8"),
        [(k.encode("utf-8"), v.encode("utf-8")) for k, v in expected],
        {k: v.encode("utf-8") if isinstance(v, str) else v
         for k, v in kwargs.items()},
        id=id_ + "_utf8_encoded_bytes",
    )
    for id_, (field, expected, kwargs)
    in {**SFP_DATA, **SFP_EXTRA_DATA}.items()
]
SFP_TEST_PARAMS = SFP_TEST_PARAMS_STR + SFP_TEST_PARAMS_BYTES


@pytest.mark.parametrize("field, expected, kwargs", SFP_TEST_PARAMS)
def test_sfp_call(field, expected, kwargs):
    sfp = SubfieldParser(**kwargs)
    result = sfp(field)
    assert isinstance(result, types.GeneratorType)
    result_list = list(result)
    assert result_list == expected


@pytest.mark.parametrize("field, expected, kwargs", SFP_TEST_PARAMS)
def test_sfp_unparse_check(field, expected, kwargs):
    sfp = SubfieldParser(**kwargs)
    valid_resynth = sfp.unparse(*expected, check=True)  # Should not raise
    assert isinstance(valid_resynth, type(field))

    # It's resynthesizable when empty is True and lower is False ...
    kwargs_etlf = {**kwargs, "empty": True, "lower": False}
    sfp_etlf = SubfieldParser(**kwargs_etlf)
    unexpected = list(sfp_etlf(field))  # Full data for resynthesis
    assert sfp_etlf.unparse(*unexpected) == field

    # ... as well as when these options makes no difference
    rf_empty = sfp.empty or all(v for k, v in unexpected[1:])
    rf_lower = not sfp.lower or all(k == k.lower() for k, v in unexpected[1:])
    is_resynthesizable = rf_empty and rf_lower
    assert is_resynthesizable == (field == valid_resynth)

    # We can also use the sfp.unparse(*unexpected),
    # mixing the SubfieldParser instance and subfields pairs
    # to resynthesize the field data, but that might raise on checking:
    # It works when the field is resynthesizable
    # and its first key is matching
    has_first = expected and sfp.fz == expected[0][0]
    is_first_case_lower_valid = kwargs.get("first", sfp.first) == sfp.first
    can_mix = is_resynthesizable and is_first_case_lower_valid
    can_check = can_mix and has_first
    assert sfp.unparse(*unexpected, check=can_check) == valid_resynth
    if not has_first:
        assert sfp.unparse(*unexpected[1:], check=can_mix) == valid_resynth
    if not can_check:
        with pytest.raises(ValueError):
            sfp.unparse(*unexpected, check=True)
