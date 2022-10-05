import distutils.version
import sys
import inspect
import pathlib


def get_caller_directory(parent_frames=0):
    """
    Find the directory where the code calling this
    function lives, or any number of jumps back up the stack

    Parameters
    ----------
    parent_frames: int
        Number of additional frames to go up in the call stack

    Returns
    -------
    directory: str
    """
    previous_frame = inspect.currentframe().f_back
    # go back more frames if desired
    for _ in range(parent_frames):
        previous_frame = previous_frame.f_back

    filename = inspect.getframeinfo(previous_frame).filename
    p = pathlib.Path(filename)
    if not p.exists():  # pragma: no cover
        # dynamically generated or interactive mode
        return None
    return str(p.parent)


def find_module_versions():
    """
    Generate a dictionary of versions of all imported modules
    by looking for __version__ or version attributes on them.

    Parameters
    ----------
    None

    Returns
    -------
    dict:
        A dictioary of the versions of all loaded modules
    """
    versions = {}
    for name, module in sys.modules.items():
        if hasattr(module, "version"):
            v = module.version
        elif hasattr(module, "__version__"):
            v = module.__version__
        else:  # pragma: no cover
            continue
        if isinstance(v, (str, distutils.version.Version)):
            versions[name] = str(v)
    return versions
