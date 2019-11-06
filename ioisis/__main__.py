import click
import ujson

from . import mst


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


if __name__ == "__main__":
    main()
