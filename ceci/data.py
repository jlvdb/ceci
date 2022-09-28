"""Rail-specific data management"""

import tables_io
from .handle import DataHandle


class DataStore(dict):
    """Class to provide a transient data store

    This class:
    1) associates data products with keys
    2) provides functions to read and write the various data produces to associated files
    """
    allow_overwrite = False

    def __init__(self, **kwargs):
        """ Build from keywords

        Note
        ----
        All of the values must be data handles of this will raise a TypeError
        """
        dict.__init__(self)
        for key, val in kwargs.items():
            self[key] = val

    def __str__(self):
        """ Override __str__ casting to deal with `TableHandle` objects in the map """
        s = "{"
        for key, val in self.items():
            s += f"  {key}:{val}\n"
        s += "}"
        return s

    def __repr__(self):
        """ A custom representation """
        s = "DataStore\n"
        s += self.__str__()
        return s

    def __setitem__(self, key, value):
        """ Override the __setitem__ to work with `TableHandle` """
        if not isinstance(value, DataHandle):
            raise TypeError(f"Can only add objects of type DataHandle to DataStore, not {type(value)}")
        check = self.get(key)
        if check is not None and not self.allow_overwrite:
            raise ValueError(f"DataStore already has an item with key {key}, of type {type(check)}, created by {check.creator}")
        dict.__setitem__(self, key, value)
        return value

    def __getattr__(self, key):
        """ Allow attribute-like parameter access """
        try:
            return self.__getitem__(key)
        except KeyError as msg:
            # Kludge to get docstrings to work
            if key in ['__objclass__']:  #pragma: no cover
                return None
            raise KeyError from msg

    def __setattr__(self, key, value):
        """ Allow attribute-like parameter setting """
        return self.__setitem__(key, value)

    def add_data(self, key, data, handle_class, path=None, creator='DataStore'):
        """ Create a handle for some data, and insert it into the DataStore """
        handle = handle_class(key, path=path, data=data, creator=creator)
        self[key] = handle
        return handle

    def read_file(self, key, handle_class, path, creator='DataStore', **kwargs):
        """ Create a handle, use it to read a file, and insert it into the DataStore """
        handle = handle_class(key, path=path, data=None, creator=creator)
        handle.read(**kwargs)
        self[key] = handle
        return handle

    def read(self, key, force=False, **kwargs):
        """ Read the data associated to a particular key """
        try:
            handle = self[key]
        except KeyError as msg:
            raise KeyError(f"Failed to read data {key} because {msg}") from msg
        return handle.read(force, **kwargs)

    def open(self, key, mode='r', **kwargs):
        """ Open and return the file associated to a particular key """
        try:
            handle = self[key]
        except KeyError as msg:
            raise KeyError(f"Failed to open data {key} because {msg}") from msg
        return handle.open(mode=mode, **kwargs)

    def write(self, key, **kwargs):
        """ Write the data associated to a particular key """
        try:
            handle = self[key]
        except KeyError as msg:
            raise KeyError(f"Failed to write data {key} because {msg}") from msg
        return handle.write(**kwargs)

    def write_all(self, force=False, **kwargs):
        """ Write all the data in this DataStore """
        for key, handle in self.items():
            local_kwargs = kwargs.get(key, {})
            if handle.is_written and not force:
                continue
            handle.write(**local_kwargs)



_DATA_STORE = DataStore()

def DATA_STORE():
    """Return the factory instance"""
    return _DATA_STORE


class TableHandle(DataHandle):
    """DataHandle for single tables of data
    """
    suffix = None

    @classmethod
    def _open(cls, path, **kwargs):
        """Open and return the associated file

        Notes
        -----
        This will simply open the file and return a file-like object to the caller.
        It will not read or cache the data
        """
        return tables_io.io.io_open(path, **kwargs)  #pylint: disable=no-member

    @classmethod
    def _read(cls, path, **kwargs):
        """Read and return the data from the associated file """
        return tables_io.read(path, **kwargs)

    @classmethod
    def _write(cls, data, path, **kwargs):
        """Write the data to the associatied file """
        return tables_io.write(data, path, **kwargs)

    @classmethod
    def _size(cls, path, **kwargs):
        return tables_io.io.getInputDataLengthHdf5(path, **kwargs)

    @classmethod
    def _iterator(cls, path, **kwargs):
        """Iterate over the data"""
        return tables_io.iteratorNative(path, **kwargs)

class Hdf5Handle(TableHandle):
    """DataHandle for a table written to HDF5"""
    suffix = 'hdf5'

    @classmethod
    def _write_chunk(cls, data, fileObj, groups, start, end, **kwargs):
        if groups is None:
            tables_io.io.writeDictToHdf5ChunkSingle(fileObj, data, start, end, **kwargs)
        else:  #pragma: no cover
            tables_io.io.writeDictToHdf5Chunk(groups, data, start, end, **kwargs)


class FitsHandle(TableHandle):
    """DataHandle for a table written to fits"""
    suffix = 'fits'


class PqHandle(TableHandle):
    """DataHandle for a parquet table"""
    suffix = 'pq'
