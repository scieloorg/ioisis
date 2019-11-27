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
    try:
        for domain in domains:
            jpype.imports.registerDomain(domain)
        yield
    finally:
        jpype.shutdownJVM()


def generator_blocking_process(func):  # noqa: C
    """Decorator to run a generator in another [blocking] process."""
    def run(func, conn_main, conn_proc, args, **kwargs):
        conn_main.close()
        try:
            conn_proc.recv()
        except EOFError:  # Pipe closed before asking the first entry
            pass
        else:
            for value in func(*args, **kwargs):
                try:
                    conn_proc.send(value)
                    conn_proc.recv()
                except EOFError:  # Broken pipe in the main process
                    break
        finally:
            conn_proc.close()

    @wraps(func)
    def wrapper(*args, **kwargs):
        conn_main, conn_proc = Pipe(duplex=True)
        proc = Process(
            target=run,
            args=[func, conn_main, conn_proc, args],
            kwargs=kwargs,
        )
        proc.start()
        try:
            conn_proc.close()
            while True:
                conn_main.send(None)  # Ask the next entry
                yield conn_main.recv()
        except EOFError:  # No more entries
            pass
        finally:
            conn_main.close()
            proc.join()

    return wrapper
