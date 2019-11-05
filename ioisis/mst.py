from collections import defaultdict
from contextlib import closing

from .java import generator_blocking_process, jvm


@generator_blocking_process
def iter_records(mst_filename):
    with jvm(domains=["bruma"], classpath="Bruma.jar"):
        from bruma.master import MasterFactory, Record
        with closing(MasterFactory.getInstance(mst_filename).open()) as mst:
            for record in mst:
                result = defaultdict(list)
                result["active"] = record.getStatus() == Record.Status.ACTIVE
                result["mfn"] = record.getMfn()
                for field in record.getFields():
                    tag = field.getId()
                    result[tag].append(field.getContent())
                yield result
