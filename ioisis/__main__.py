from codecs import escape_decode
from functools import reduce
from inspect import signature
from io import BytesIO
import signal
import sys

import click
import ujson

from . import bruma, iso, mst
from .fieldutils import nest_decode, nest_encode, SubfieldParser, \
                        tl2record, record2tl, utf8_fix_nest_decode


DEFAULT_JSONL_ENCODING = "utf-8"
INPUT_PATH = object()
OUTPUT_PATH = object()


def apply_decorators(*decorators):
    """Decorator that applies the decorators in reversed order."""
    return lambda func: reduce(lambda f, d: d(f), decorators[::-1], func)


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
    type=click.Choice(["field", "pairs", "nest"], case_sensitive=False),
    default="field",
    help="Mode of JSONL record structure processing "
         "and of field/subfield parsing.",
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
        help='Prepend the "mfn" field.',
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


@click.group()
def main():
    """ISIS data converter using the ioisis Python library."""
    try:  # Fix BrokenPipeError by opening a new fake standard output
        signal.signal(signal.SIGPIPE,
                      lambda signum, frame: setattr(sys, "stdout", BytesIO()))
    except (AttributeError, ValueError):
        pass  # No SIGPIPE in this OS


@main.command("bruma-mst2jsonl")
@apply_decorators(*metadata_filtering_options)
@jsonl_mode_option
@apply_decorators(*subfield_options)
@file_arg_enc_option("mst", INPUT_PATH, mst.DEFAULT_MST_ENCODING)
@file_arg_enc_option("jsonl", "w", DEFAULT_JSONL_ENCODING)
def bruma_mst2jsonl(mst_input, jsonl_output, mst_encoding, mode, **kwargs):
    """MST+XRF to JSON Lines based on a Bruma (requires Java)."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    kwargs_menc = {key: kwargs[key].decode(mst_encoding)
                   for key in ["prefix", "first"]}
    sfp = kw_call(SubfieldParser, **{**kwargs, **kwargs_menc})
    itl = kw_call(bruma.iter_tl, mst_input, **kwargs, encoding=mst_encoding)
    for tl_decoded in itl:
        record = tl2record(tl_decoded, sfp, mode)
        ujson.dump(
            record, jsonl_output,
            ensure_ascii=ensure_ascii,
            escape_forward_slashes=False,
        )
        jsonl_output.write("\n")
        jsonl_output.flush()


@main.command()
@apply_decorators(*[op for op in mst_options if op.args[0] != "default_shift"])
@mst_ibp_option
@apply_decorators(*metadata_filtering_options)
@jsonl_mode_option
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
        record = tl2record(tl, sfp, mode)
        ujson.dump(
            decode(record, encoding=mst_encoding),
            jsonl_output,
            ensure_ascii=ensure_ascii,
            escape_forward_slashes=False,
        )
        jsonl_output.write("\n")
        jsonl_output.flush()


@main.command()
@apply_decorators(*mst_options)
@jsonl_mode_option
@apply_decorators(*subfield_options)
@subfield_unparse_check_option
@file_arg_enc_option("jsonl", "r", DEFAULT_JSONL_ENCODING)
@file_arg_enc_option("mst", OUTPUT_PATH, mst.DEFAULT_MST_ENCODING)
def jsonl2mst(jsonl_input, mst_output, mst_encoding, mode, **kwargs):
    """JSON Lines to ISIS/FFI Master File Format."""
    sfp = kw_call(SubfieldParser, **kwargs, check=kwargs["sfcheck"])
    mst_sc = kw_call(mst.StructCreator, **kwargs)
    def generate_records():
        for line in jsonl_input:
            record = nest_encode(ujson.loads(line), encoding=mst_encoding)
            tl = record2tl(record, sfp, mode)
            yield mst.tl2con(tl)
    with open(mst_output, "wb") as mst_file:
        mst_sc.build_stream(generate_records(), mst_file)


@main.command()
@apply_decorators(*iso_options)
@apply_decorators(*metadata_filtering_options)
@jsonl_mode_option
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
        record = tl2record(tl, sfp, mode)
        ujson.dump(
            decode(record, encoding=iso_encoding),
            jsonl_output,
            ensure_ascii=ensure_ascii,
            escape_forward_slashes=False,
        )
        jsonl_output.write("\n")
        jsonl_output.flush()


@main.command()
@apply_decorators(*iso_options)
@jsonl_mode_option
@apply_decorators(*subfield_options)
@subfield_unparse_check_option
@file_arg_enc_option("jsonl", "r", DEFAULT_JSONL_ENCODING)
@file_arg_enc_option("iso", "wb", iso.DEFAULT_ISO_ENCODING)
def jsonl2iso(jsonl_input, iso_output, iso_encoding, mode, **kwargs):
    """JSON Lines to ISO2709."""
    record_struct = kw_call(iso.create_record_struct, **kwargs)
    sfp = kw_call(SubfieldParser, **kwargs, check=kwargs["sfcheck"])
    for line in jsonl_input:
        record = nest_encode(ujson.loads(line), encoding=iso_encoding)
        tl = record2tl(record, sfp, mode)
        iso_bytes = iso.tl2bytes(tl, record_struct=record_struct)
        iso_output.write(iso_bytes)
        iso_output.flush()


if __name__ == "__main__":
    main()
