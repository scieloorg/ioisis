import io

from ioisis.iso import con2dict, DEFAULT_RECORD_STRUCT, \
                       iter_raw_tl, iter_records
from ioisis.fieldutils import nest_decode, tl2record


def test_tag_zero():
    iso_data = DEFAULT_RECORD_STRUCT.build({
        "dir": [{"tag": b"000"}],
        "fields": [b"data"],
    })
    con = DEFAULT_RECORD_STRUCT.parse(iso_data)
    expected = {"0": ["data"]}
    assert con2dict(con, encoding="ascii") == expected


def test_converting_iter_raw_tl_result_to_behave_like_iter_records():
    iso_data = DEFAULT_RECORD_STRUCT.build({
        "dir": [{"tag": b"100"}, {"tag": b"001"}, {"tag": b"010"}],
        "fields": ["™©®".encode("utf-8"), b"data ", b" here"],
    })
    tl, = iter_raw_tl(io.BytesIO(iso_data))
    record, = iter_records(io.BytesIO(iso_data), encoding="utf-8")
    assert record == nest_decode(tl2record(tl), encoding="utf-8")
