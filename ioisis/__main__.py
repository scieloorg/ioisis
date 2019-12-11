from codecs import escape_decode
from functools import reduce
from io import BytesIO
import signal
import sys

import click
import ujson

from . import iso, mst


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


@click.group()
def main():
    """ISIS data converter using the ioisis Python library."""
    try:  # Fix BrokenPipeError by opening a new fake standard output
        signal.signal(signal.SIGPIPE,
                      lambda signum, frame: setattr(sys, "stdout", BytesIO()))
    except (AttributeError, ValueError):
        pass  # No SIGPIPE in this OS


@main.command()
@file_arg_enc_option("mst", INPUT_PATH, mst.DEFAULT_MST_ENCODING)
@file_arg_enc_option("jsonl", "w", DEFAULT_JSONL_ENCODING)
def mst2jsonl(mst_input, jsonl_output, jsonl_encoding, mst_encoding):
    """MST+XRF to JSON Lines."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    for record in mst.iter_records(mst_input, encoding=mst_encoding):
        ujson.dump(
            record, jsonl_output,
            ensure_ascii=ensure_ascii,
            escape_forward_slashes=False,
        )
        jsonl_output.write("\n")
        jsonl_output.flush()


@main.command()
@apply_decorators(*iso_options)
@file_arg_enc_option("iso", "rb", iso.DEFAULT_ISO_ENCODING)
@file_arg_enc_option("jsonl", "w", DEFAULT_JSONL_ENCODING)
def iso2jsonl(iso_input, jsonl_output, iso_encoding, jsonl_encoding,
              **iso_kwargs):
    """ISO2709 to JSON Lines."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    record_struct = iso.create_record_struct(**iso_kwargs)
    for record in iso.iter_records(iso_input,
                                   record_struct=record_struct,
                                   encoding=iso_encoding):
        ujson.dump(
            record, jsonl_output,
            ensure_ascii=ensure_ascii,
            escape_forward_slashes=False,
        )
        jsonl_output.write("\n")
        jsonl_output.flush()


@main.command()
@apply_decorators(*iso_options)
@file_arg_enc_option("jsonl", "r", DEFAULT_JSONL_ENCODING)
@file_arg_enc_option("iso", "wb", iso.DEFAULT_ISO_ENCODING)
def jsonl2iso(jsonl_input, iso_output, iso_encoding, jsonl_encoding,
              **iso_kwargs):
    """JSON Lines to ISO2709."""
    record_struct = iso.create_record_struct(**iso_kwargs)
    for line in jsonl_input:
        record_dict = ujson.loads(line)
        iso_output.write(iso.dict2bytes(
            record_dict,
            record_struct=record_struct,
            encoding=iso_encoding,
        ))
        iso_output.flush()


if __name__ == "__main__":
    main()
