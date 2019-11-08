# IOISIS - I/O tools for converting ISIS data in Python

This is a Python library with command line interface
intended to access data from ISIS database files
and convert file formats.


## Command Line Interface (CLI)

To use the CLI command, use `ioisis` or `python -m ioisis`.
Examples:

```bash
# Read file.mst (and file.xrf) to a JSONL in the standard output stream
ioisis mst2jsonl file.mst

# Convert file.iso to an ASCII file.jsonl
ioisis iso2jsonl --jenc ascii file.iso file.jsonl

# Convert file.jsonl to file.iso where the JSON lines are like
# {"tag": ["field", ...], ...}
ioisis jsonl2iso file.jsonl file.iso

# Indirectly, convert file.mst to file.iso using jq
ioisis mst2jsonl file.mst \
| jq -c 'del(.active) | del(.mfn)' \
| ioisis jsonl2iso - file.iso
```

By default, the input and output are the standard streams,
but for MST+XRF input, where the MST must be given
and the matching XRF will be found based on the file name.

Try `ioisis --help` for more information.


## Library

To load ISIS data, you can use the `iter_records` function
of the respective module:

```python
from ioisis import iso, mst

# For MST files, you must use the filename
for record_dict in mst.iter_records("file.mst"):
    ...

# For ISO files, you can either use a file name
# or any file-like object open in "rb" mode
with open("file.iso", "rb") as raw_iso_file:
    for record_dict in iso.iter_records(raw_iso_file):
        ...
```

One can generate a single ISO record from a dict of data:

```python
>>> from ioisis import iso
>>> iso.dict2bytes({"1": ["testing"], "8": ["it"]})
b'000610000000000490004500001000800000008000300008#testing#it##\n'

```
