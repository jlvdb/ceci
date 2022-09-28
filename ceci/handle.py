import os
import copy

class DataHandle:
    """Class to act as a handle for a bit of data.  Associating it with a file and
    providing tools to read & write it to that file

    Parameters
    ----------
    tag : str
        The tag under which this data handle can be found in the store
    data : any or None
        The associated data
    path : str or None
        The path to the associated file
    creator : str or None
        The name of the stage that created this data handle
    """
    suffix = ''

    def __init__(self, tag, data=None, path=None, creator=None, provenance=None):
        """Constructor """
        self.tag = tag
        self.data = data
        self.path = path
        self.creator = creator
        self.provenance = provenance
        self.fileObj = None
        self.groups = None
        self.partial = False

    @property
    def provenance(self):
        """Return the provenance"""
        return self._provenance

    @provenance.setter
    def provenance(self, provenance):
        # always copy the provenance
        self._provenance = copy.deepcopy(provenance)

    def open(self, **kwargs):
        """Open and return the associated file

        Notes
        -----
        This will simply open the file and return a file-like object to the caller.
        It will not read or cache the data
        """
        if self.path is None:
            raise ValueError("DataHandle.open() called but path has not been specified")
        self.fileObj = self._open(self.path, **kwargs)
        return self.fileObj

    @classmethod
    def _open(cls, path, **kwargs):
        raise NotImplementedError("DataHandle._open")  #pragma: no cover

    def close(self, **kwargs):  #pylint: disable=unused-argument
        """Close """
        self.fileObj = None

    def read(self, force=False, **kwargs):
        """Read and return the data from the associated file """
        if self.data is not None and not force:
            return self.data
        self.set_data(self._read(self.path, **kwargs))
        return self.data

    def __call__(self, **kwargs):
        """Return the data, re-reading the fill if needed"""
        if self.has_data and not self.partial:
            return self.data
        return self.read(force=True, **kwargs)

    @classmethod
    def _read(cls, path, **kwargs):
        raise NotImplementedError("DataHandle._read")  #pragma: no cover

    def write(self, **kwargs):
        """Write the data to the associatied file """
        if self.path is None:
            raise ValueError("TableHandle.write() called but path has not been specified")
        if self.data is None:
            raise ValueError(f"TableHandle.write() called for path {self.path} with no data")
        outdir = os.path.dirname(os.path.abspath(self.path))
        if not os.path.exists(outdir):  #pragma: no cover
            os.makedirs(outdir, exist_ok=True)
        return self._write(self.data, self.path, **kwargs)

    @classmethod
    def _write(cls, data, path, **kwargs):
        raise NotImplementedError("DataHandle._write")  #pragma: no cover

    def initialize_write(self, data_lenght, **kwargs):
        """Initialize file to be written by chunks"""
        if self.path is None:  #pragma: no cover
            raise ValueError("TableHandle.write() called but path has not been specified")
        self.groups, self.fileObj = self._initialize_write(self.data, self.path, data_lenght, **kwargs)

    @classmethod
    def _initialize_write(cls, data, path, data_lenght, **kwargs):
        raise NotImplementedError("DataHandle._initialize_write") #pragma: no cover

    def write_chunk(self, start, end, **kwargs):
        """Write the data to the associatied file """
        if self.data is None:
            raise ValueError(f"TableHandle.write_chunk() called for path {self.path} with no data")
        if self.fileObj is None:
            raise ValueError(f"TableHandle.write_chunk() called before open for {self.tag} : {self.path}")
        return self._write_chunk(self.data, self.fileObj, self.groups, start, end, **kwargs)


    @classmethod
    def _write_chunk(cls, data, fileObj, groups, start, end, **kwargs):
        raise NotImplementedError("DataHandle._write_chunk")  #pragma: no cover

    def finalize_write(self, **kwargs):
        """Finalize and close file written by chunks"""
        if self.fileObj is None:  #pragma: no cover
            raise ValueError(f"TableHandle.finalize_wite() called before open for {self.tag} : {self.path}")
        self._finalize_write(self.data, self.fileObj, **kwargs)

    @classmethod
    def _finalize_write(cls, data, fileObj, **kwargs):
        raise NotImplementedError("DataHandle._finalize_write")  #pragma: no cover

    def iterator(self, **kwargs):
        """Iterator over the data"""
        #if self.data is not None:
        #    for i in range(1):
        #        yield i, -1, self.data
        return self._iterator(self.path, **kwargs)

    def set_data(self, data, partial=False):
        """Set the data for a chunk, and set the partial flag to true"""
        self.data = data
        self.partial = partial

    def size(self, **kwargs):
        """Return the size of the data associated to this handle"""
        return self._size(self.path, **kwargs)

    @classmethod
    def _size(cls, path, **kwargs):
        raise NotImplementedError("DataHandle._size")  #pragma: no cover

    @classmethod
    def _iterator(cls, path, **kwargs):
        raise NotImplementedError("DataHandle._iterator")  #pragma: no cover

    @property
    def has_data(self):
        """Return true if the data for this handle are loaded """
        return self.data is not None

    @property
    def has_path(self):
        """Return true if the path for the associated file is defined """
        return self.path is not None

    @property
    def is_written(self):
        """Return true if the associated file has been written """
        if self.path is None:
            return False
        return os.path.exists(self.path)

    def __str__(self):
        s = f"{type(self)} "
        if self.has_path:
            s += f"{self.path}, ("
        else:
            s += "None, ("
        if self.is_written:
            s += "w"
        if self.has_data:
            s += "d"
        s += ")"
        return s

    @classmethod
    def make_name(cls, tag):
        """Construct and return file name for a particular data tag """
        if cls.suffix:
            return f"{tag}.{cls.suffix}"
        else:
            return tag  #pragma: no cover
