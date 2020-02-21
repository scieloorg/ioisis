from collections import defaultdict
from contextlib import closing
from hashlib import sha256
import os
from urllib.request import urlopen

from .java import generator_blocking_process, jvm
from .mst import DEFAULT_MST_ENCODING


BRUMA_URL = "https://github.com/scieloorg/isis2json/raw/def7327/lib/Bruma.jar"
BRUMA_JAR = os.path.join(os.path.expanduser("~"), ".ioisis", "Bruma.jar")
BRUMA_HASH = "a68c3f21ad98a21de49b2eb1c75ccc63bb46f336c1b285ea7fc6d128d29fc57d"


class BrumaCheckError(Exception):
    pass


def check_bruma():
    """Check if Bruma.jar is valid, and download it if it's missing."""
    if not os.path.exists(BRUMA_JAR):
        download_bruma()
    if not os.path.isfile(BRUMA_JAR):
        raise BrumaCheckError("Bruma.jar isn't a file")
    with open(BRUMA_JAR, "rb") as raw_bruma_file:
        if sha256(raw_bruma_file.read()).hexdigest() != BRUMA_HASH:
            raise BrumaCheckError("Invalid Bruma.jar file")


def download_bruma():
    """Simply download BRUMA_URL to BRUMA_JAR."""
    os.makedirs(os.path.dirname(BRUMA_JAR), exist_ok=True)
    with open(BRUMA_JAR, "wb") as raw_bruma_file:
        raw_bruma_file.write(urlopen(BRUMA_URL).read())


@generator_blocking_process
def iter_records(mst_filename, encoding=DEFAULT_MST_ENCODING):
    check_bruma()
    with jvm(domains=["bruma"], classpath=BRUMA_JAR):
        from bruma.master import MasterFactory, Record
        mf = MasterFactory.getInstance(mst_filename).setEncoding(encoding)
        with closing(mf.open()) as mst:
            for record in mst:
                result = defaultdict(list)
                result["active"] = record.getStatus() == Record.Status.ACTIVE
                result["mfn"] = record.getMfn()
                for field in record.getFields():
                    tag = field.getId()
                    result[tag].append(field.getContent())
                yield result


@generator_blocking_process
def iter_tl(
    mst_filename,
    encoding=DEFAULT_MST_ENCODING,
    only_active=True,
    prepend_mfn=False,
    prepend_status=False,
):
    check_bruma()
    with jvm(domains=["bruma"], classpath=BRUMA_JAR):
        from bruma.master import MasterFactory
        mf = MasterFactory.getInstance(mst_filename).setEncoding(encoding)
        with closing(mf.open()) as mst:
            for record in mst:
                status_name = record.getStatus().name()
                if status_name != "ACTIVE" and (only_active or
                                                status_name == "PHYDEL"):
                    continue
                result = []
                if prepend_mfn:
                    result.append(("mfn", "%d" % record.getMfn()))
                if prepend_status:
                    status = {"ACTIVE": "0", "LOGDEL": "1"}[status_name]
                    result.append(("status", status))
                for field in record.getFields():
                    result.append((field.getId(), field.getContent()))
                yield result
