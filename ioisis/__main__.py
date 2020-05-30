import csv
from codecs import escape_decode
from functools import reduce
from inspect import signature
from itertools import groupby
from io import BytesIO
import signal
import sys

import click
import ujson

from . import bruma, iso, mst
from .fieldutils import nest_decode, nest_encode, SubfieldParser, \
                        tl2record, record2tl, utf8_fix_nest_decode, \
                        DEFAULT_FTF_TEMPLATE, FieldTagFormatter, tl2con


DEFAULT_CSV_ENCODING = "utf-8"
DEFAULT_JSONL_ENCODING = "utf-8"
INPUT_PATH = object()
OUTPUT_PATH = object()
CMODE_HEADERS = {
    "tidy": ["mfn", "index", "tag", "data"],
    "stidy": ["mfn", "index", "tag", "sindex", "sub", "data"],
}


def apply_decorators(*decorators):
    """Decorator that applies the decorators in reversed order."""
    return lambda func: reduce(lambda f, d: d(f), decorators[::-1], func)


def change_defaults(**kwargs):
    """Callback function factory for changing parameter defaults
    in a click command.
    """
    def callback_function(ctx, param, value):
        if ctx.default_map is None:
            ctx.default_map = {}
        if value:
            ctx.default_map.update(kwargs)
    return callback_function


def option(*args, **kwargs):
    """Same to click.option, but saves the args/kwargs for filtering."""
    result = click.option(*args, **kwargs)
    result.args = args
    result.kwargs = kwargs
    return result


def encoding_option(file_ext, default, **kwargs):
    ctx_attr = file_ext + "_encoding"
    return click.option(
        ctx_attr,
        f"--{file_ext[0]}enc",
        default=default,
        show_default=True,
        callback=lambda ctx, param, value:
            setattr(ctx, ctx_attr, value) or value,
        is_eager=True,
        help=f"{file_ext.upper()} file encoding.",
        **kwargs,
    )


def file_arg_enc_option(file_ext, mode, default_encoding):
    arg_kwargs = {}
    if mode is INPUT_PATH:
        arg_name = file_ext + "_input"
        arg_kwargs["type"] = click.Path(
            dir_okay=False,
            resolve_path=True,
            allow_dash=False,
            readable=True,
            writable=False,
        )
    elif mode is OUTPUT_PATH:
        arg_name = file_ext + "_output"
        arg_kwargs["type"] = click.Path(
            dir_okay=False,
            resolve_path=True,
            allow_dash=False,
            readable=False,
            writable=True,
        )
    else:
        arg_kwargs["default"] = "-"
        if "w" in mode:
            arg_name = file_ext + "_output"
        else:
            arg_name = file_ext + "_input"
        if "b" in mode:
            arg_kwargs["type"] = click.File(mode)
        else:
            ctx_attr = file_ext + "_encoding"
            arg_kwargs["callback"] = lambda ctx, param, value: \
                click.File(mode, encoding=getattr(ctx, ctx_attr))(value)

    return apply_decorators(
        encoding_option(file_ext, default=default_encoding),
        click.argument(arg_name, **arg_kwargs)
    )


def iso_bytes_option_with_default(*args, **kwargs):
    return option(
        *args,
        metavar="BYTES",
        show_default=True,
        callback=lambda ctx, param, value:
            escape_decode(value.encode(ctx.iso_encoding))[0],
        **kwargs
    )


def kw_call(func, *args, **kwargs):
    """Call ``func`` without the extra unknown keywords."""
    sig_keys = signature(func).parameters.keys()
    return func(*args, **{k: kwargs[k] for k in sig_keys if k in kwargs})


def read_csv_decoded_record(stream, cmode):
    icr = iter(csv.reader(stream))
    header = next(icr)
    cmode_header = CMODE_HEADERS[cmode]
    cmode_types = [int if k in ["mfn", "index", "sindex"] else str
                   for k in cmode_header]
    order = [header.index(k) for k in cmode_header]
    mfn_idx = header.index("mfn")
    for mfn, grp in groupby(icr, key=lambda row: row[mfn_idx]):
        kic = list(zip(cmode_header, order, cmode_types))
        yield [{k: cmtype(row[idx]) for k, idx, cmtype in kic} for row in grp]


def read_csv_raw_tl(stream, cmode, sfp, encoding, prepend_mfn):
    for decoded_record in read_csv_decoded_record(stream, cmode):
        record = nest_encode(decoded_record, encoding=encoding)
        yield record2tl(record, sfp, cmode, prepend_mfn)


def read_json_decoded_record(stream, mode):
    if mode in ["tidy", "stidy"]:
        fields = map(ujson.loads, stream)
        for mfn, grp in groupby(fields, key=lambda field: field["mfn"]):
            yield list(grp)
    else:
        for line in stream:
            yield ujson.loads(line)


def read_json_raw_tl(stream, mode, sfp, encoding, prepend_mfn):
    for decoded_record in read_json_decoded_record(stream, mode):
        record = nest_encode(decoded_record, encoding=encoding)
        yield record2tl(record, sfp, mode, prepend_mfn)


def write_json(decoded_record, stream, ensure_ascii=False):
    if isinstance(decoded_record, list):  # Tidy format
        for item in decoded_record:
            write_json(item, stream, ensure_ascii=ensure_ascii)
    else:  # Dict data
        ujson.dump(
            decoded_record, stream,
            ensure_ascii=ensure_ascii,
            escape_forward_slashes=False,
        )
        stream.write("\n")
        stream.flush()


iso_options = [
    iso_bytes_option_with_default(
        "field_terminator", "--ft",
        default=iso.DEFAULT_FIELD_TERMINATOR,
        help="ISO Field terminator",
    ),
    iso_bytes_option_with_default(
        "record_terminator", "--rt",
        default=iso.DEFAULT_RECORD_TERMINATOR,
        help="ISO Record terminator",
    ),
    option(
        "line_len", "--line",
        default=iso.DEFAULT_LINE_LEN,
        show_default=True,
        help="Line size to wrap the raw ISO data into several lines. "
             "If zero, performs no line splitting.",
    ),
    iso_bytes_option_with_default(
        "newline", "--eol",
        default=iso.DEFAULT_NEWLINE,
        help="End of line character/string for ISO line splitting. "
             "Ignored if --line=0.",
    ),
]


jsonl_mode_option = click.option(
    "--mode", "-m",
    type=click.Choice(["field", "pairs", "nest", "inest", "tidy", "stidy"],
                      case_sensitive=False),
    default="field",
    show_default=True,
    callback=lambda ctx, param, value: setattr(ctx, "mode", value) or value,
    is_eager=True,
    help="Mode for JSONL record structure processing "
         "and for its field/subfield parsing. "
         'The "tidy" and "stidy" are tabular modes '
         "where each row is a field or a subfield, respectively. "
         "The remaining modes put each record as a single JSON "
         "with the field as the key, "
         'like {"111": [f0], "123": [f1, f2]}, '
         "with different strategies on how the field values appear "
         "(f0, f1 and f2 in the example). "
         'The "field" mode uses the raw field value string. '
         'The "pairs" mode splits field '
         "into key-value pairs of subfields "
         'like [["_", "start"], ["t", "data"]] '
         'instead of using a raw string like "start^tdata". '
         'The "nest" and "inest" modes are like the "pairs" mode, '
         "but uses an object for each field value "
         "instead of an array of pairs (arrays) of strings. "
         "As an object, "
         "subfields with the same key might get overwritten: "
         'the "nest" uses the last value '
         "with the duplicated subfield key, "
         'whereas the "inest" keeps the first value.'
)

csv_mode_option = click.option(
    "--cmode", "-M",
    type=click.Choice(["tidy", "stidy"], case_sensitive=False),
    default="tidy",
    show_default=True,
    help="Mode for CSV record structure processing, "
         "where a record is split in a tabular format "
         "with a line for each field (tidy) "
         "or for each subfield (stidy).",
)


field_tag_format_option = click.option(
    "--ftf",
    metavar="BYTES",
    default=DEFAULT_FTF_TEMPLATE,
    show_default=True,
    callback=lambda ctx, param, value:
        FieldTagFormatter(escape_decode(value.encode("ascii"))[0],
                          int_tags="mst" in ctx.command.name),
    help="Field tag format template for parsing/rendering. "
         "It can include: "
         "%d: tag as a number; "
         "%r: tag as a raw string (as it appears in the input); "
         "%z: tag without leading zeros even if it's not a number; "
         "%i: field index as a number. "
         "The %d and %i formats accept a number in the middle "
         "to set the tag string size, like in printf. "
         "For example: "
         "%04d would parse/render the tag with 4 characters, "
         "like 0012; "
         "%5i would parse/render the index with 5 characters "
         "with leading whitespace."
)


subfield_options = [
    option(
        "--prefix",
        metavar="BYTES",
        default=b"^",
        show_default=True,
        callback=lambda ctx, param, value:
            escape_decode(value.encode("ascii"))[0],
        help="Subfield prefix mark."
    ),
    option(
        "--length",
        default=1,
        show_default=True,
        help="Subfield key length in bytes."
    ),
    option(
        "--lower/--no-lower",
        default=True,
        show_default=True,
        help="Put subfield keys in lower case, "
             "making them case insensitive."
    ),
    option(
        "--first",
        metavar="BYTES",
        default=b"_",
        show_default=True,
        callback=lambda ctx, param, value:
            escape_decode(value.encode("ascii"))[0],
        help="Key to be used for the first keyless subfield."
    ),
    option(
        "--empty/--no-empty",
        default=False,
        show_default=True,
        help="Keep subfield pairs with empty values."
    ),
    option(
        "--number/--no-number",
        default=True,
        show_default=True,
        help="Add a number suffix to the subfield keys "
             "that have already appeared before for the same field. "
             "The suffix numbers start in 1."
    ),
    option(
        "--zero/--no-zero",
        default=False,
        show_default=True,
        help='Add the "0" suffix '
             "to the first of each distinct subfield key. "
             "Has no effect when --no-number."
    ),
]


subfield_unparse_check_option = click.option(
    "--sfcheck/--no-sfcheck",
    default=True,
    show_default=True,
    help="Check if the subfield unparsing rules match its contents, "
         "that is, check if the field generated from the subfields "
         "would produce the same subfields."
)


metadata_filtering_options = [
    option(
        "only_active", "--only-active/--all",
        default=True,
        show_default=True,
        help='Select only records whose status is "0" (active).',
    ),
    option(
        "--prepend-mfn/--no-mfn",
        default=False,
        show_default=True,
        callback=lambda ctx, param, value:
            value or getattr(ctx, "mode", None) in ["tidy", "stidy"],
        help='Prepend the "mfn" field. '
             'This option has no effect in the "tidy"/"stidy" modes',
    ),
    option(
        "--prepend-status/--no-status",
        default=False,
        show_default=True,
        help='Prepend the "status" field from the record metadata, '
             "whose value is either "
             '"0" (active) or "1" (logically deleted).',
    ),
]


mst_options = [
    option(
        "endianness", "--end",
        default=mst.DEFAULT_ENDIANNESS,
        show_default=True,
        type=click.Choice(["little", "big"]),
        help="Byte order endianness for 16/32 bits integer numbers. "
             'Little endian is known as "swapped" in CISIS/Bruma.',
    ),
    option(
        "endianness", "--le",
        flag_value="little",
        is_eager=True,
        help="Same to --end=little.",
    ),
    option(
        "endianness", "--be",
        flag_value="big",
        is_eager=True,
        help="Same to --end=big.",
    ),
    option(
        "--format",
        default=mst.DEFAULT_FORMAT,
        show_default=True,
        type=click.Choice(["isis", "ffi"]),
        help="Leader and directory format mode in master files. "
             "In the ISIS format mode, "
             "the addressing and length fields "
             "(MFRL, BASE, POS and LEN) have 2 bytes, "
             "whereas in the FFI format mode they have 4 bytes.",
    ),
    option(
        "format", "--isis",
        flag_value="isis",
        is_eager=True,
        help="Same to --format=isis.",
    ),
    option(
        "format", "--ffi",
        flag_value="ffi",
        is_eager=True,
        help="Same to --format=ffi.",
    ),
    option(
        "--lockable/--no-locks",
        default=mst.DEFAULT_LOCKABLE,
        show_default=True,
        help="Multi-user locking, "
             "where the MFRL sign is a RLOCK (record lock flag), "
             "MFCXX2 is the DELOCK (Data entry lock / RLOCK counter) "
             "and MFCXX3 is the EWLOCK (Exclusive write lock). "
             "MFRL will be interpreted as unsigned if --no-locks, "
             "effectively increasing the maximum record size "
             "to twice plus one.",
    ),
    option(
        "default_shift", "--shift",
        default=mst.DEFAULT_SHIFT,
        show_default=True,
        help="MSTXL value, the number of XRF bit shift steps. "
             "It affects the minimum possible modulus "
             "for record alignment in the MST file, "
             "and it's part of the control record. "
             "To get the standard ISIS behavior, this should be 0, "
             "and to get the CISIS FFI behavior, this should be 3, "
             "although 6 is the most common choice for large files "
             "in both format modes.",
    ),
    option(
        "--shift4is3/--shift4isnt3",
        default=mst.DEFAULT_SHIFT4IS3,
        show_default=True,
        help="Legacy shifting interpretation "
             "where there's no 4 bits shifting, "
             "and MSTXL=4 should be replaced by MSTXL=3.",
    ),
    option(
        "--min-modulus",
        default=mst.DEFAULT_MIN_MODULUS,
        show_default=True,
        help="Smallest modulus value for record alignment. "
             "Due to XRF shifting, the actual modulus is 2**MSTXL "
             "(2 raised to the power of MSTXL), "
             "unless this value is higher than that. "
             "The ISIS standard define this as 2. "
             "This option makes it possible "
             "to disable the 2 bytes (WORD) alignment of records "
             "by setting it as 1.",
    ),
    option(
        "--packed/--unpacked",
        default=mst.DEFAULT_PACKED,
        show_default=True,
        help="Control the leader and FFI directory alignment. "
             "If --packed, "
             "there should be no padding/filler/slack bytes in these. "
             "If --unpacked, there should be a 4-bytes alignment, "
             "adding 2 filler bytes in the leader "
             "(after the MFRL in ISIS, after the BASE in FFI) "
             "and 2 filler bytes after the TAG "
             "in FFI directory entries. "
             "These are also known as align0/align2 in Bruma "
             "(i.e., with the number of padding bytes), "
             "and as PC/LINUX (or Windows/Linux) in CISIS "
             "(because it used to be compiled in Windows "
             " with -fpack-struct=1, and without it in Linux), "
             "though this option has nothing to do operating system.",
    ),
    option(
        "--filler",
        metavar="HEX_BYTE",
        default="%02X" % ord(mst.DEFAULT_FILLER),
        show_default=True,
        callback=lambda ctx, param, value:
            bytes([int(value, 16)]) if value else None,
        help="Character code in hexadecimal for unset filler options "
             "that doesn't have a specific default.",
    ),
    option(
        "--control-filler",
        metavar="HEX_BYTE",
        callback=lambda ctx, param, value:
            bytes([int(value, 16)]) if value else None,
        help="Filler character code "
             "for the trailing bytes of the control record. "
             "The CISIS source code tells "
             'it should be "FF" for Unisys, and "00" otherwise.',
    ),
    option(
        "--slack-filler",
        metavar="HEX_BYTE",
        callback=lambda ctx, param, value:
            bytes([int(value, 16)]) if value else None,
        help="Filler character code for alignment "
             "in the leader and the directory of all records. "
             "Has no effect when --packed.",
    ),
    option(
        "--block-filler",
        metavar="HEX_BYTE",
        callback=lambda ctx, param, value:
            bytes([int(value, 16)]) if value else None,
        help="Filler character code "
             "for the trailing recordless bytes of a block.",
    ),
    option(
        "--record-filler",
        metavar="HEX_BYTE",
        default="%02X" % ord(mst.DEFAULT_RECORD_FILLER),
        show_default=True,
        callback=lambda ctx, param, value:
            bytes([int(value, 16)]) if value else None,
        help="Filler character code for the trailing record data.",
    ),
    option(
        "--control-len",
        default=mst.DEFAULT_CONTROL_LEN,
        show_default=True,
        help="Control record length, at least 32. "
             "It must be multiple of the modulus.",
    ),
]

mst_ibp_option = click.option(
    "--ibp",
    type=click.Choice(["check", "ignore", "store"]),
    default=mst.DEFAULT_IBP,
    show_default=True,
    help="Invalid block padding content/size, "
         "which might appear as a residual of some previous content "
         "when the MST file gets updated in place. "
         'If "store", the contents are stored in the previous record.',
)


utf8_fix_option = click.option(
    "utf8_fix", "--utf8",
    is_flag=True,
    default=False,
    help="Decode the input data with UTF-8 if possible, "
         "using the given input encoding as a fallback.",
)


xylose_option = click.option(
    "--xylose",
    is_eager=True,  # Because --mode is eager as well
    is_flag=True,
    expose_value=False,
    callback=change_defaults(mode="inest", ftf="v%z"),
    help='Same to "--mode=inest --ftf=v%z".',
)


class ShortNameAliasGroup(click.Group):

    def get_command(self, ctx, cmd_name):
        names = self.list_commands(ctx)
        if cmd_name in names:
            return super().get_command(ctx, cmd_name)
        alias_map = {
            "".join(el[0] for el in name.replace("2", "-2-").split("-")): name
            for name in names
        }
        if cmd_name in alias_map:
            return super().get_command(ctx, alias_map[cmd_name])


@click.command(cls=ShortNameAliasGroup)
def main():
    """ISIS data converter using the ioisis Python library.

    All command names can also be called by using only the first letter
    of the file formats they're describing
    (e.g. "m2j" instead of "mst2jsonl"),
    where "bruma-" gets replaced by a single "b".
    """
    try:  # Fix BrokenPipeError by opening a new fake standard output
        signal.signal(signal.SIGPIPE,
                      lambda signum, frame: setattr(sys, "stdout", BytesIO()))
    except (AttributeError, ValueError):
        pass  # No SIGPIPE in this OS


@main.command("bruma-mst2jsonl")
@apply_decorators(*metadata_filtering_options)
@jsonl_mode_option
@field_tag_format_option
@xylose_option
@apply_decorators(*subfield_options)
@file_arg_enc_option("mst", INPUT_PATH, mst.DEFAULT_MST_ENCODING)
@file_arg_enc_option("jsonl", "w", DEFAULT_JSONL_ENCODING)
def bruma_mst2jsonl(mst_input, jsonl_output, mst_encoding, mode, **kwargs):
    """MST+XRF to JSON Lines based on Bruma (requires Java)."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    kwargs_menc = {key: kwargs[key].decode(mst_encoding)
                   for key in ["prefix", "first"]}
    sfp = kw_call(SubfieldParser, **{**kwargs, **kwargs_menc})
    itl = kw_call(bruma.iter_tl, mst_input, **kwargs, encoding=mst_encoding)
    for tl_decoded in itl:
        record = tl2record(tl_decoded, sfp, mode)
        write_json(record, jsonl_output, ensure_ascii=ensure_ascii)


@main.command()
@apply_decorators(*[op for op in mst_options if op.args[0] != "default_shift"])
@mst_ibp_option
@apply_decorators(*metadata_filtering_options)
@jsonl_mode_option
@field_tag_format_option
@xylose_option
@apply_decorators(*subfield_options)
@utf8_fix_option
@file_arg_enc_option("mst", "rb", mst.DEFAULT_MST_ENCODING)
@file_arg_enc_option("jsonl", "w", DEFAULT_JSONL_ENCODING)
def mst2jsonl(mst_input, jsonl_output, mst_encoding, mode, utf8_fix, **kwargs):
    """ISIS/FFI Master File Format to JSON Lines."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    mst_sc = kw_call(mst.StructCreator, **kwargs)
    sfp = kw_call(SubfieldParser, **kwargs)
    decode = utf8_fix_nest_decode if utf8_fix else nest_decode
    for tl in kw_call(mst_sc.iter_raw_tl, mst_input, **kwargs):
        record = decode(tl2record(tl, sfp, mode), encoding=mst_encoding)
        write_json(record, jsonl_output, ensure_ascii=ensure_ascii)


@main.command()
@apply_decorators(*mst_options)
@jsonl_mode_option
@field_tag_format_option
@xylose_option
@apply_decorators(*subfield_options)
@subfield_unparse_check_option
@file_arg_enc_option("jsonl", "r", DEFAULT_JSONL_ENCODING)
@file_arg_enc_option("mst", OUTPUT_PATH, mst.DEFAULT_MST_ENCODING)
def jsonl2mst(jsonl_input, mst_output, mst_encoding, mode, ftf, **kwargs):
    """JSON Lines to ISIS/FFI Master File Format."""
    sfp = kw_call(SubfieldParser, **kwargs, check=kwargs["sfcheck"])
    mst_sc = kw_call(mst.StructCreator, **kwargs)
    tl_gen = read_json_raw_tl(
        stream=jsonl_input,
        mode=mode,
        sfp=sfp,
        encoding=mst_encoding,
        prepend_mfn=True,
    )
    with open(mst_output, "wb") as mst_file:
        con_gen = (tl2con(tl, ftf) for tl in tl_gen)
        mst_sc.build_stream(con_gen, mst_file)


@main.command()
@apply_decorators(*iso_options)
@apply_decorators(*metadata_filtering_options)
@jsonl_mode_option
@field_tag_format_option
@xylose_option
@apply_decorators(*subfield_options)
@utf8_fix_option
@file_arg_enc_option("iso", "rb", iso.DEFAULT_ISO_ENCODING)
@file_arg_enc_option("jsonl", "w", DEFAULT_JSONL_ENCODING)
def iso2jsonl(iso_input, jsonl_output, iso_encoding, mode, utf8_fix, **kwargs):
    """ISO2709 to JSON Lines."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    kwargs["record_struct"] = kw_call(iso.create_record_struct, **kwargs)
    sfp = kw_call(SubfieldParser, **kwargs)
    decode = utf8_fix_nest_decode if utf8_fix else nest_decode
    for tl in kw_call(iso.iter_raw_tl, iso_input, **kwargs):
        record = decode(tl2record(tl, sfp, mode), encoding=iso_encoding)
        write_json(record, jsonl_output, ensure_ascii=ensure_ascii)


@main.command()
@apply_decorators(*iso_options)
@jsonl_mode_option
@field_tag_format_option
@xylose_option
@apply_decorators(*subfield_options)
@subfield_unparse_check_option
@file_arg_enc_option("jsonl", "r", DEFAULT_JSONL_ENCODING)
@file_arg_enc_option("iso", "wb", iso.DEFAULT_ISO_ENCODING)
def jsonl2iso(jsonl_input, iso_output, iso_encoding, mode, ftf, **kwargs):
    """JSON Lines to ISO2709."""
    record_struct = kw_call(iso.create_record_struct, **kwargs)
    sfp = kw_call(SubfieldParser, **kwargs, check=kwargs["sfcheck"])
    tl_gen = read_json_raw_tl(
        stream=jsonl_input,
        mode=mode,
        sfp=sfp,
        encoding=iso_encoding,
        prepend_mfn=False,
    )
    for tl in tl_gen:
        iso_bytes = record_struct.build(tl2con(tl, ftf))
        iso_output.write(iso_bytes)
        iso_output.flush()


@main.command("bruma-mst2csv")
@apply_decorators(*[op for op in metadata_filtering_options
                    if not op.args[0].startswith("--prepend-mfn")])
@csv_mode_option
@field_tag_format_option
@apply_decorators(*subfield_options)
@file_arg_enc_option("mst", INPUT_PATH, mst.DEFAULT_MST_ENCODING)
@file_arg_enc_option("csv", "w", DEFAULT_CSV_ENCODING)
def bruma_mst2csv(mst_input, csv_output, mst_encoding, cmode, **kwargs):
    """MST+XRF to CSV based on Bruma (requires Java)."""
    kwargs["prepend_mfn"] = True
    kwargs_menc = {key: kwargs[key].decode(mst_encoding)
                   for key in ["prefix", "first"]}
    sfp = kw_call(SubfieldParser, **{**kwargs, **kwargs_menc})
    itl = kw_call(bruma.iter_tl, mst_input, **kwargs, encoding=mst_encoding)
    csv_writer = csv.writer(csv_output)
    header = CMODE_HEADERS[cmode]
    csv_writer.writerow(header)
    for tl_decoded in itl:
        record = tl2record(tl_decoded, sfp, cmode)
        csv_writer.writerows([row[k] for k in header] for row in record)


@main.command()
@apply_decorators(*[op for op in mst_options if op.args[0] != "default_shift"])
@mst_ibp_option
@apply_decorators(*[op for op in metadata_filtering_options
                    if not op.args[0].startswith("--prepend-mfn")])
@csv_mode_option
@field_tag_format_option
@apply_decorators(*subfield_options)
@utf8_fix_option
@file_arg_enc_option("mst", "rb", mst.DEFAULT_MST_ENCODING)
@file_arg_enc_option("csv", "w", DEFAULT_CSV_ENCODING)
def mst2csv(mst_input, csv_output, mst_encoding, cmode, utf8_fix, **kwargs):
    """ISIS/FFI Master File Format to CSV."""
    kwargs["prepend_mfn"] = True
    mst_sc = kw_call(mst.StructCreator, **kwargs)
    sfp = kw_call(SubfieldParser, **kwargs)
    decode = utf8_fix_nest_decode if utf8_fix else nest_decode
    csv_writer = csv.writer(csv_output)
    header = CMODE_HEADERS[cmode]
    csv_writer.writerow(header)
    for tl in kw_call(mst_sc.iter_raw_tl, mst_input, **kwargs):
        record = decode(tl2record(tl, sfp, cmode), encoding=mst_encoding)
        csv_writer.writerows([row[k] for k in header] for row in record)


@main.command()
@apply_decorators(*mst_options)
@csv_mode_option
@field_tag_format_option
@apply_decorators(*subfield_options)
@subfield_unparse_check_option
@file_arg_enc_option("csv", "r", DEFAULT_CSV_ENCODING)
@file_arg_enc_option("mst", OUTPUT_PATH, mst.DEFAULT_MST_ENCODING)
def csv2mst(csv_input, mst_output, mst_encoding, cmode, ftf, **kwargs):
    """CSV to ISIS/FFI Master File Format."""
    sfp = kw_call(SubfieldParser, **kwargs, check=kwargs["sfcheck"])
    mst_sc = kw_call(mst.StructCreator, **kwargs)
    tl_gen = read_csv_raw_tl(
        stream=csv_input,
        cmode=cmode,
        sfp=sfp,
        encoding=mst_encoding,
        prepend_mfn=True,
    )
    with open(mst_output, "wb") as mst_file:
        con_gen = (tl2con(tl, ftf) for tl in tl_gen)
        mst_sc.build_stream(con_gen, mst_file)


@main.command()
@apply_decorators(*iso_options)
@apply_decorators(*[op for op in metadata_filtering_options
                    if not op.args[0].startswith("--prepend-mfn")])
@csv_mode_option
@field_tag_format_option
@apply_decorators(*subfield_options)
@utf8_fix_option
@file_arg_enc_option("iso", "rb", iso.DEFAULT_ISO_ENCODING)
@file_arg_enc_option("csv", "w", DEFAULT_CSV_ENCODING)
def iso2csv(iso_input, csv_output, iso_encoding, cmode, utf8_fix, **kwargs):
    """ISO2709 to CSV."""
    kwargs["prepend_mfn"] = True
    kwargs["record_struct"] = kw_call(iso.create_record_struct, **kwargs)
    sfp = kw_call(SubfieldParser, **kwargs)
    decode = utf8_fix_nest_decode if utf8_fix else nest_decode
    csv_writer = csv.writer(csv_output)
    header = CMODE_HEADERS[cmode]
    csv_writer.writerow(header)
    for tl in kw_call(iso.iter_raw_tl, iso_input, **kwargs):
        record = decode(tl2record(tl, sfp, cmode), encoding=iso_encoding)
        csv_writer.writerows([row[k] for k in header] for row in record)


@main.command()
@apply_decorators(*iso_options)
@csv_mode_option
@field_tag_format_option
@apply_decorators(*subfield_options)
@subfield_unparse_check_option
@file_arg_enc_option("csv", "r", DEFAULT_CSV_ENCODING)
@file_arg_enc_option("iso", "wb", iso.DEFAULT_ISO_ENCODING)
def csv2iso(csv_input, iso_output, iso_encoding, cmode, ftf, **kwargs):
    """CSV to ISO2709."""
    record_struct = kw_call(iso.create_record_struct, **kwargs)
    sfp = kw_call(SubfieldParser, **kwargs, check=kwargs["sfcheck"])
    tl_gen = read_csv_raw_tl(
        stream=csv_input,
        cmode=cmode,
        sfp=sfp,
        encoding=iso_encoding,
        prepend_mfn=False,
    )
    for tl in tl_gen:
        iso_bytes = record_struct.build(tl2con(tl, ftf))
        iso_output.write(iso_bytes)
        iso_output.flush()


@main.command()
@jsonl_mode_option
@csv_mode_option
@apply_decorators(*subfield_options)
@subfield_unparse_check_option
@file_arg_enc_option("jsonl", "r", DEFAULT_JSONL_ENCODING)
@file_arg_enc_option("csv", "w", DEFAULT_CSV_ENCODING)
def jsonl2csv(jsonl_input, csv_output, mode, cmode, **kwargs):
    """JSON Lines to CSV."""
    kwargs_menc = {key: kwargs[key].decode(csv_output.encoding)
                   for key in ["prefix", "first"]}
    sfp = kw_call(SubfieldParser, **{**kwargs, **kwargs_menc},
                  check=kwargs["sfcheck"])
    record_gen = read_json_decoded_record(stream=jsonl_input, mode=mode)
    csv_writer = csv.writer(csv_output)
    header = CMODE_HEADERS[cmode]
    csv_writer.writerow(header)
    last_mfn = 0
    for jrecord in record_gen:  # Encoding is handled by the file I/O
        if mode not in ["tidy", "stidy"]:
            if "mfn" in jrecord:  # Put it at the beginning
                mfn = jrecord.pop("mfn")
                last_mfn = max(last_mfn, mfn)
            else:  # Create a new MFN for it
                last_mfn += 1
                mfn = last_mfn
            jrecord = {"mfn": [str(mfn)], **jrecord}
        tl = record2tl(jrecord, sfp, mode, prepend_mfn=True)
        crecord = tl2record(tl, sfp, cmode)
        csv_writer.writerows([row[k] for k in header] for row in crecord)


@main.command()
@jsonl_mode_option
@csv_mode_option
@apply_decorators(*subfield_options)
@subfield_unparse_check_option
@file_arg_enc_option("csv", "r", DEFAULT_CSV_ENCODING)
@file_arg_enc_option("jsonl", "w", DEFAULT_JSONL_ENCODING)
def csv2jsonl(csv_input, jsonl_output, mode, cmode, **kwargs):
    """CSV to JSON Lines."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    kwargs_menc = {key: kwargs[key].decode(jsonl_output.encoding)
                   for key in ["prefix", "first"]}
    sfp = kw_call(SubfieldParser, **{**kwargs, **kwargs_menc},
                  check=kwargs["sfcheck"])
    record_gen = read_csv_decoded_record(stream=csv_input, cmode=cmode)
    for crecord in record_gen:  # Encoding is handled by the file I/O
        tl = record2tl(crecord, sfp, cmode, prepend_mfn=True)
        jrecord = tl2record(tl, sfp, mode)
        write_json(jrecord, jsonl_output, ensure_ascii=ensure_ascii)


if __name__ == "__main__":
    main()
