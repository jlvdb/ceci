import os
import pytest
import pickle
import numpy as np
import pandas as pd
from types import GeneratorType

import ceci
from ceci.stage import PipelineStage
from descformats import DataStore, TableHandle, DataHandle, PqHandle, Hdf5Handle, FitsHandle

CECIDIR = os.path.abspath(os.path.join(os.path.dirname(ceci.__file__), '..'))

#def test_data_file():    
#    with pytest.raises(ValueError) as errinfo:
#        df = DataFile('dummy', 'x')

class ColumnMapper(PipelineStage):
    """Utility stage that remaps the names of columns.

    Notes
    -----
    1. This operates on pandas dataframs in parquet files.

    2. In short, this does:
    `output_data = input_data.rename(columns=self.config.columns, inplace=self.config.inplace)`

    """
    name = 'ColumnMapper'
    config_options = PipelineStage.config_options.copy()
    config_options.update(chunk_size=100_000, columns=dict, inplace=False)
    inputs = [('input', PqHandle)]
    outputs = [('output', PqHandle)]

    def __init__(self, args, comm=None):
        PipelineStage.__init__(self, args, comm=comm)

    def run(self):
        data = self.get_data('input', allow_missing=True)
        out_data = data.rename(columns=self.config.columns, inplace=self.config.inplace)
        if self.config.inplace:  #pragma: no cover
            out_data = data
        self.add_data('output', out_data)

    def __repr__(self):  # pragma: no cover
        printMsg = "Stage that applies remaps the following column names in a pandas DataFrame:\n"
        printMsg += "f{str(self.config.columns)}"
        return printMsg

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        """Return a table with the columns names changed

        Parameters
        ----------
        sample : pd.DataFrame
            The data to be renamed

        Returns
        -------
        pd.DataFrame
            The degraded sample
        """
        self.set_data('input', data)
        self.run()
        return self.get_handle('output')
    
    def input_iterator(self, tag, **kwargs):
        """Iterate the input assocated to a particular tag

        Parameters
        ----------
        tag : str
            The tag (from cls.inputs or cls.outputs) for this data

        kwargs : dict[str, Any]
            These will be passed to the Handle's iterator method
        """
        handle = self.get_handle(tag, allow_missing=True)
        if not handle.has_data:  #pragma: no cover
            handle.read()
        if self.config.hdf5_groupname:
            self._input_length = handle.size(groupname=self.config.hdf5_groupname)
            kwcopy = dict(groupname=self.config.hdf5_groupname,
                          chunk_size=self.config.chunk_size,
                          rank=self.rank,
                          parallel_size=self.size)
            kwcopy.update(**kwargs)
            return handle.iterator(**kwcopy)
        else:  #pragma: no cover
            test_data = self.get_data('input')
            s = 0
            e = len(list(test_data.items())[0][1])
            self._input_length=e
            iterator=[[s, e, test_data]]
            return iterator


def test_stage_interface():
    DS = PipelineStage.data_store
    DS.clear()
    
    datapath = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816.hdf5')
    cm = ColumnMapper.make_stage(input=datapath, chunk_size=1000,
                                 hdf5_groupname='photometry', columns=dict(id='bob'))

    with pytest.raises(KeyError) as errinfo:
        cm.get_handle('input')

    th = cm.add_handle('input', path=datapath)
    data = cm.get_data('input') 
    cm.set_data("input", data, path=datapath)

    DS.clear()    
    th2 = Hdf5Handle('data', path=datapath)
    data2 = th2.read()
    cm.set_data("input", data=data2)

    DS.clear()    
    th3 = Hdf5Handle('data', path=datapath)
    data3 = th2.read()
    cm.add_handle("input", data=data3)

    DS.clear()    
    th4 = Hdf5Handle('data', path=datapath)
    data4 = th2.read()
    cm.add_data("input", data=data4)

    DS.clear()    
    cm.set_data("input", data=None, path=datapath, do_read=True)

    
def test_data_hdf5_iter():

    DS = PipelineStage.data_store
    DS.clear()
    
    datapath = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816.hdf5')
    cm = ColumnMapper.make_stage(input=datapath, chunk_size=1000,
                                 hdf5_groupname='photometry', columns=dict(id='bob'))
    x = cm.input_iterator('input')

    assert isinstance(x, GeneratorType)

    for i, xx in enumerate(x):
        assert xx[0] == i*1000
        assert xx[1] - xx[0] <= 1000

