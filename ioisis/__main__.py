from io import BytesIO
import signal
import sys

import click
import ujson

from . import iso, mst


DEFAULT_JSONL_ENCODING = "utf-8"


@click.group()
def main():
    """ISIS data converter using the ioisis Python library."""
    try:  # Fix BrokenPipeError by opening a new fake standard output
        signal.signal(signal.SIGPIPE,
                      lambda signum, frame: setattr(sys, "stdout", BytesIO()))
    except (AttributeError, ValueError):
        pass  # No SIGPIPE in this OS


@main.command()
@click.option(
    "jsonl_encoding", "--jenc",
    default=DEFAULT_JSONL_ENCODING,
    show_default=True,
    callback=lambda ctx, param, value: setattr(ctx, "jsonl_encoding", value),
    is_eager=True,
    help="JSONL file encoding.",
)
@click.option(
    "mst_encoding", "--menc",
    default=mst.DEFAULT_MST_ENCODING,
    show_default=True,
    help="MST file encoding.",
)
@click.argument(
    "mst_input",
    type=click.Path(dir_okay=False, resolve_path=True, allow_dash=False),
)
@click.argument(
    "jsonl_output",
    callback=lambda ctx, param, value:
        click.File("w", encoding=ctx.jsonl_encoding)(value),
    default="-",
)
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


@main.command()
@click.option(
    "iso_encoding", "--ienc",
    default=iso.DEFAULT_ISO_ENCODING,
    show_default=True,
    help="ISO file encoding.",
)
@click.option(
    "jsonl_encoding", "--jenc",
    default=DEFAULT_JSONL_ENCODING,
    show_default=True,
    callback=lambda ctx, param, value: setattr(ctx, "jsonl_encoding", value),
    is_eager=True,
    help="JSONL file encoding.",
)
@click.argument("iso_input", type=click.File("rb"), default="-")
@click.argument(
    "jsonl_output",
    callback=lambda ctx, param, value:
        click.File("w", encoding=ctx.jsonl_encoding)(value),
    default="-",
)
def iso2jsonl(iso_input, jsonl_output, iso_encoding, jsonl_encoding):
    """ISO2709 to JSON Lines."""
    ensure_ascii = jsonl_output.encoding.lower() == "ascii"
    for record in iso.iter_records(iso_input):
        ujson.dump(
            record, jsonl_output,
            ensure_ascii=ensure_ascii,
            escape_forward_slashes=False,
        )
        jsonl_output.write("\n")


@main.command()
@click.option(
    "iso_encoding", "--ienc",
    default=iso.DEFAULT_ISO_ENCODING,
    show_default=True,
    help="ISO file encoding.",
)
@click.option(
    "jsonl_encoding", "--jenc",
    default=DEFAULT_JSONL_ENCODING,
    show_default=True,
    callback=lambda ctx, param, value: setattr(ctx, "jsonl_encoding", value),
    is_eager=True,
    help="JSONL file encoding.",
)
@click.argument(
    "jsonl_input",
    callback=lambda ctx, param, value:
        click.File("r", encoding=ctx.jsonl_encoding)(value),
    default="-",
)
@click.argument("iso_output", type=click.File("wb"), default="-")
def jsonl2iso(jsonl_input, iso_output, iso_encoding, jsonl_encoding):
    """JSON Lines to ISO2709."""
    for line in jsonl_input:
        record_dict = ujson.loads(line)
        iso_output.write(iso.dict2bytes(record_dict, encoding=iso_encoding))


if __name__ == "__main__":
    main()
