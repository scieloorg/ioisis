import click
import ujson

from . import iso, mst


@click.group()
def main():
    """ISIS data converter using the ioisis Python library."""


@main.command()
@click.argument(
    "mst_input",
    type=click.Path(dir_okay=False, resolve_path=True, allow_dash=False),
)
@click.argument("jsonl_output", type=click.File("w"), default="-")
def mst2jsonl(mst_input, jsonl_output):
    """MST+XRF to JSON Lines."""
    for record in mst.iter_records(mst_input):
        ujson.dump(
            record, jsonl_output,
            ensure_ascii=False,
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
@click.argument("iso_input", type=click.File("rb"), default="-")
@click.argument("jsonl_output", type=click.File("w"), default="-")
def iso2jsonl(iso_input, jsonl_output, iso_encoding):
    """ISO2709 to JSON Lines."""
    for record in iso.iter_records(iso_input):
        ujson.dump(
            record, jsonl_output,
            ensure_ascii=False,
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
@click.argument("jsonl_input", type=click.File("r"), default="-")
@click.argument("iso_output", type=click.File("wb"), default="-")
def jsonl2iso(jsonl_input, iso_output, iso_encoding):
    """JSON Lines to ISO2709."""
    for line in jsonl_input:
        record_dict = ujson.loads(line)
        iso_output.write(iso.dict2bytes(record_dict, encoding=iso_encoding))


if __name__ == "__main__":
    main()
