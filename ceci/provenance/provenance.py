from . import git
from . import utils
import sys
import uuid
import socket
import getpass
import pathlib
import datetime
import functools
import numpy as np
import copy
import desc_dict_io

# Some useful constants
unknown_value = "UNKNOWN"
provenance_group = "provenance"
base_section = "base"
config_section = "config"
input_id_section = "input_id"
input_path_section = "input_path"
git_section = "git"
versions_section = "versions"
comments_section = "comments"


def writer_method(method):
    """Do some book-keeping to turn a provenance method into a writer method

    We put this decorator around all the methods that write
    provenance to a file, so that they all generate, remove,
    and return a unique ID for each new file they write to.

    It's not intended for users.
    """
    # This makes the decorator "well-behaved" so that it
    # doesn't change the name of the function when printed,
    # etc.
    @functools.wraps(method)
    def wrapped_method(self, *args, **kwargs):
        # Record it in the provenance object
        file_id = self.generate_file_id()

        # I was a bit confused at the need to include
        # self here, but it seems to be required
        try:
            method(self, *args, **kwargs)
        finally:
            # At the end, remove it from the Provenance, because it will not
            # be relevant for future files
            del self.provenance[base_section, "file_id"]
        # But return it; this is mainly to help testing
        return file_id

    return wrapped_method


def writable_value(x):
    """Convert to a string, integer or float for persistance"""
    if isinstance(x, (int, np.integer, float, np.floating)):
        return x
    return str(x)


class Provenance:
    """Collects, generates, reads, and writes provenance information.

    This object can be used like a dictionary with two keys:
    provenance[category, key] = value
    """

    def __init__(self, code_dir=None, parent_frames=0):
        """Create an empty provenance object"""
        self.code_dir = code_dir or utils.get_caller_directory(parent_frames + 1)
        self.provenance = {}
        self.comments = []

    def copy(self):
        """Copy self"""
        cls = self.__class__
        cp = cls(code_dir=self.code_dir)
        cp.provenance = copy.deepcopy(self.provenance)
        cp.comments = copy.deepcopy(self.comments)
        return cp

    # Generation methods
    # ------------------
    def generate(
        self, user_config=None, input_files=None, comments=None, directory=None
    ):
        """
        Generate a new set of provenance.

        After calling this the provenance object will contain:
            - the date, time, and place of creation
            - the user and domain name
            - all python modules already imported anywhere that have a version number
            - git info about the directory where this instance was created
            - sys.argv
            - a config dict passed by the caller
            - a dict of input files passed by the caller
            - any comments we want to add.

        Parameters
        ----------
        user_config: dict or None
            Optional input configuration options
        input_files: dict or None
            Optional name_for_file: file_path dict
        comments: list or None
            Optional comments to include.  Not intended to be machine-readable
        directory: str or None
            Optional directory in which to run git information
        """
        # Record various core pieces of information
        self._add_core_info()
        self._add_git_info(directory)
        self._add_module_versions()
        self._add_argv_info()

        # Add user inputs
        if input_files is not None:
            for name, path in input_files.items():
                self.add_input_file(name, path)

        # Add any specific items given by the user
        if user_config is not None:
            for key, value in user_config.items():
                self[config_section, key] = writable_value(value)

        if comments is not None:
            for comment in comments:
                self.add_comment(comment)

    # Core methods called in generate above
    # -------------------------------------
    def _add_core_info(self):
        self[base_section, "process_id"] = uuid.uuid4().hex
        self[base_section, "domain"] = socket.getfqdn()
        self[base_section, "creation"] = datetime.datetime.now().isoformat()
        self[base_section, "user"] = getpass.getuser()

    def _add_argv_info(self):
        for i, arg in enumerate(sys.argv):
            self[base_section, f"argv_{i}"] = arg

    def _add_git_info(self, directory):
        # Add some git information
        self[git_section, "diff"] = git.diff(directory)
        self[git_section, "head"] = git.current_revision()

    def _add_module_versions(self):
        for module, version in utils.find_module_versions().items():
            self[versions_section, module] = version

    def add_input_file(self, name, path):
        """
        Tell the provenance the name and path to one of your input files
        so it can be recorded correctly in the output.

        Parameters
        ----------
        name: str
            A tag or name representing the file

        path: str or pathlib.Path
            The path to the file
        """
        # get the absolute form path to the file
        path = str(pathlib.Path(path).absolute().resolve())

        # Save it in ourselves
        self[input_path_section, name] = path
        # If the file was saved with its own provenance then it will have its own
        # unique file_id.  Try to record that ID.  The file may be some other type,
        # or not have provenance, so ignore any errors here.
        try:
            self[input_id_section, name] = self.get(path, base_section, "file_id")
        except:
            self[input_id_section, name] = unknown_value

    def add_comment(self, comment):
        """
        Add a text comment.

        Comments with line breaks will be split into separate comments.

        Parameters
        ----------
        comment: str
            Comment to include
        """
        for c in comment.split("\n"):
            self.comments.append(c)

    # Dictionary methods
    # ------------------
    def __getitem__(self, section_name):
        section, name = section_name
        return self.provenance[section, name]

    def __setitem__(self, section_name, value):
        section, name = section_name
        self.provenance[section, name] = value

    def __delitem__(self, section_name):
        section, name = section_name
        del self.provenance[section, name]

    def update(self, d):
        """
        Update the provenance from a dictionary.

        The dictionary keys should be tuples of (category, key).
        The values can be any basic type.

        Parameters
        ----------
        d: dict or mapping
            The dict to update from.
        """
        for (section, name), value in d.items():
            self.provenance[section, name] = value

    # Generic I/O Methods
    # -----------
    @writer_method
    def write(self, f, suffix=None):
        """
        Write provenance to a named file, guessing the file type from its suffix.

        Use the various write_* methods intead to write to a file you have already
        opened, or if the file suffix does not match the type.

        Parameters
        ----------
        f: str or writeable object
        suffix: str
            Must be supplied if f is a file-like object

        Returns
        -------
        str
            The newly-assigned file ID
        """
        return desc_dict_io.write(
            self.provenance,
            f,
            provenance_group,
            suffix=suffix,
            comments=self.comments,
            comments_section=comments_section,
        )

    def read(self, filename):
        """
        Read all provenance from any supported file type, guessing
        the file type from its suffix.

        If the suffix does not match the type you can use one of the specific read_
        methods instead.  You can also pass open file objects directly to those methods.

        Parameters
        ----------
        filename: str

        Returns
        -------
        None
        """
        d, com = desc_dict_io.read(
            filename,
            provenance_group,
            comments_section=comments_section,
        )
        self.update(d)
        self.comments.extend(com)

    @classmethod
    def get(cls, filename, section, key):
        """
        Get a single item of provenance from any supported file type, guessing
        the file type from its suffix.

        If the suffix does not match the type you can use one of the specific get_
        methods instead.  You can also pass open file objects directly to those methods.

        Parameters
        ----------
        filename: str

        section: str

        key: str

        Returns
        -------
        value: any
            The native value of the key in this value
        """
        return desc_dict_io.get(
            filename,
            provenance_group,
            section,
            key,
        )

    def to_string_dict(self):
        """Convert to a dict with string keys and values"""
        d = {f"{s}/{k}": str(v) for (s, k), v in self.provenance.items()}
        for i, c in enumerate(self.comments):
            d[f"comment_{i}"] = c
        return d

    def generate_file_id(self):
        """Generate a unique ID for a file"""
        file_id = uuid.uuid4().hex
        self[base_section, "file_id"] = file_id
        return file_id
