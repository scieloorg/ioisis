from contextlib import contextmanager
from functools import wraps
from multiprocessing import Process, Pipe


@contextmanager
def jvm(domains=(), classpath=None, use_str=True):
    """JVM context using JPype.

    This must not be called more than once in a single process!
    """
    import jpype
    import jpype.imports
    jpype.startJVM(classpath=classpath, convertStrings=use_str)
    for domain in domains:
        jpype.imports.registerDomain(domain)
    yield
    jpype.shutdownJVM()


def generator_blocking_process(func):
    """Decorator to run a generator in another [blocking] process."""
    def run(func, conn, args, **kwargs):
        conn.recv()
        for value in func(*args, **kwargs):
            conn.send(value)
            conn.recv()
        conn.close()

    @wraps(func)
    def wrapper(*args, **kwargs):
        conn_main, conn_proc = Pipe(duplex=True)
        proc = Process(target=run, args=[func, conn_proc, args], kwargs=kwargs)
        proc.start()
        try:
            conn_proc.close()
            while True:
                conn_main.send(None)  # Ask the next entry
                yield conn_main.recv()
        except EOFError:  # No more entries
            pass
        finally:
            proc.join()

    return wrapper
