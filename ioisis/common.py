from functools import update_wrapper
from inspect import isgeneratorfunction, signature


def should_be_file(file_argname, mode="rb"):
    """Decorator to enforce the given argument is a file-like object.
    If it isn't, it will be seen as a filename,
    and it'll be replaced by the open file with the given mode.
    """
    def decorator(func):
        if isgeneratorfunction(func):
            def wrapper(*args, **kwargs):
                bound_args = signature(func).bind(*args, **kwargs)
                file_arg = bound_args.arguments[file_argname]
                if hasattr(file_arg, "read"):  # Already a file, nothing to do
                    yield from func(*args, **kwargs)
                    return
                with open(file_arg, mode) as file_obj:  # "Cast" name to file
                    bound_args.arguments[file_argname] = file_obj
                    yield from func(*bound_args.args, **bound_args.kwargs)
        else:
            raise NotImplementedError
        return update_wrapper(wrapper, func)
    return decorator
