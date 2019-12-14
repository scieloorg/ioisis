from codecs import escape_decode
from functools import reduce
from inspect import signature
from io import BytesIO
import signal
import sys

import click
import ujson

from . import iso, mst
from .fieldutils import nest_decode, nest_encode, SubfieldParser, \
                        tl2record, record2tl


DEFAULT_JSONL_ENCODING = "utf-8"
INPUT_PATH = object()


def apply_decorators(*decorators):
    """Decorator that applies the decorators in reversed order."""
    return lambda func: reduce(lambda f, d: d(f), decorators[::-1], func)


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
    arg_name = file_ext + "_input"
    arg_kwargs = {}
    if mode is INPUT_PATH:
        arg_kwargs["type"] = click.Path(
            dir_okay=False,
            resolve_path=True,
            allow_dash=False,
        )
    else:
        arg_kwargs["default"] = "-"
        if "w" in mode:
            arg_name = file_ext + "_output"
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
    return click.option(
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
    click.option(
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
    click.option(
        "--prefix",
        metavar="BYTES",
        default=b"^",
        show_default=True,
        callback=lambda ctx, param, value:
            escape_decode(value.encode("ascii"))[0],
        help="Subfield prefix mark."
    ),
    click.option(
        "--length",
        default=1,
        show_default=True,
        help="Subfield key length in bytes."
    ),
    click.option(
        "--lower/--no-lower",
        default=True,
        show_default=True,
        help="Put subfield keys in lower case, "
             "making them case insensitive."
    ),
    click.option(
        "--first",
        metavar="BYTES",
        default=b"_",
        show_default=True,
        callback=lambda ctx, param, value:
            escape_decode(value.encode("ascii"))[0],
        help="Key to be used for the first keyless subfield."
    ),
    click.option(
        "--empty/--no-empty",
        default=False,
        show_default=True,
        help="Keep subfield pairs with empty values."
    ),
    click.option(
        "--number/--no-number",
        default=True,
        show_default=True,
        help="Add a number suffix to the subfield keys "
             "that have already appeared before for the same field. "
             "The suffix numbers start in 1."
    ),
    click.option(
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


mst_metadata_filtering_options = [
    click.option(
        "only_active", "--only-active/--all",
        default=False,
        show_default=True,
        help="Select only records whose status is ACTIVE.",
    ),
    click.option(
        "--prepend-active/--no-active",
        default=False,
        show_default=True,
        help='Prepend a synthesized "active" field from the status, '
             'whose value might be "0" (false) or "1" (true).',
    ),
    click.option(
        "--prepend-mfn/--no-mfn",
        default=False,
        show_default=True,
        help='Prepend the "mfn" field.',
    ),
    click.option(
        "--prepend-status/--no-status",
        default=False,
        show_default=True,
        help='Prepend the "status" field from the record metadata, '
             'whose value might be "ACTIVE", "LOGDEL", or "PHYDEL".',
    ),
]


@click.group()
def main():
    """ISIS data converter using the ioisis Python library."""
    try:  # Fix BrokenPipeError by opening a new fake standard output
        signal.signal(signal.SIGPIPE,
                      lambda signum, frame: setattr(sys, "stdout", BytesIO()))
    except (AttributeError, ValueError):
        pass  # No SIGPIPE in this OS


@main.command()
@apply_decorators(*mst_metadata_filtering_options)
@jsonl_mode_option
@apply_decorators(*subfield_options)
@file_arg_enc_option("mst", INPUT_PATH, mst.DEFAULT_MST_ENCODING)
@file_arg_enc_option("jsonl", "w", DEFAULT_JSONL_ENCODING)
def mst2jsonl(mst_input, jsonl_output, mst_encoding, mode, **kwargs):
    """MST+XRF to JSON Lines."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    kwargs_menc = {key: kwargs[key].decode(mst_encoding)
                   for key in ["prefix", "first"]}
    sfp = kw_call(SubfieldParser, **{**kwargs, **kwargs_menc})
    itl = kw_call(mst.iter_tl, mst_input, **kwargs, encoding=mst_encoding)
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
@apply_decorators(*iso_options)
@jsonl_mode_option
@apply_decorators(*subfield_options)
@file_arg_enc_option("iso", "rb", iso.DEFAULT_ISO_ENCODING)
@file_arg_enc_option("jsonl", "w", DEFAULT_JSONL_ENCODING)
def iso2jsonl(iso_input, jsonl_output, iso_encoding, mode, **kwargs):
    """ISO2709 to JSON Lines."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    record_struct = kw_call(iso.create_record_struct, **kwargs)
    sfp = kw_call(SubfieldParser, **kwargs)
    for tl in iso.iter_raw_tl(iso_input, record_struct=record_struct):
        record = tl2record(tl, sfp, mode)
        ujson.dump(
            nest_decode(record, encoding=iso_encoding),
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
