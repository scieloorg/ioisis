from click.testing import CliRunner

from ioisis.__main__ import iso2jsonl, jsonl2iso


simple_example_jsonl = (
    b'{"1":["a"]}\n'
    b'{"10":["test","one"],"11":["two"]}\n'
    b'{}\n'
    b'{"1":["x"],"10":["y","z"],"100":["aa","bbb","cccc"]}\n'
)

simple_example_iso = (
    b"000400000000000370004500001000200000#a##\n"
    b"000750000000000610004500"
        b"010000500000010000400005011000400009#test#one#two##\n"
    b"000260000000000250004500##\n"
    b"001160000000000970004500"
        b"00100020000001000020000201000020000410000030000610000040\n"
    b"0009100000500013#x#y#z#aa#bbb#cccc##\n"
)


def test_jsonl2iso_simple_example_on_standard_streams():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(jsonl2iso, input=simple_example_jsonl)
    assert result.exit_code == 0
    assert result.stdout_bytes == simple_example_iso
    assert result.stderr_bytes == b""


def test_iso2jsonl_simple_example_on_standard_streams():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(iso2jsonl, input=simple_example_iso)
    assert result.exit_code == 0
    assert result.stdout_bytes == simple_example_jsonl
    assert result.stderr_bytes == b""
