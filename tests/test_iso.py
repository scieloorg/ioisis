from ioisis.iso import con2dict, DEFAULT_RECORD_STRUCT


def test_tag_zero():
    iso_data = DEFAULT_RECORD_STRUCT.build({
        "dir": [{"tag": b"000"}],
        "fields": [b"data"],
    })
    con = DEFAULT_RECORD_STRUCT.parse(iso_data)
    expected = {"0": ["data"]}
    assert con2dict(con, encoding="ascii") == expected
