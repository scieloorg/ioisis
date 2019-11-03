from contextlib import contextmanager


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
