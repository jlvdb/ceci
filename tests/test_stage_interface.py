import os
import pytest
import pickle
import numpy as np
import pandas as pd
from types import GeneratorType

import ceci
from ceci.stage import PipelineStage
import descformats
from descformats import DataStore, TableHandle, DataHandle, PqHandle, Hdf5Handle, FitsHandle

DATADIR = os.path.abspath(os.path.join(os.path.dirname(descformats.__file__), 'data'))

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
    outputs = [('output', Hdf5Handle)]

    def __init__(self, args, comm=None):
        PipelineStage.__init__(self, args, comm=comm)

    def run(self):
        data = self.get_data('input', allow_missing=True)
        out_data = data.rename(columns=self.config.columns, inplace=self.config.inplace)
        if self.config.inplace:  #pragma: no cover
            out_data = data
        self.add_data('output', out_data)

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


class ProvChecker(PipelineStage):
    name = 'ProvChecker'
    config_options = PipelineStage.config_options.copy()
    inputs = [('input', PqHandle)]
    outputs = []

    def run(self):
        data = self.get_data('input', allow_missing=True)
        f = self.open_input('input', wrapper=True)
        assert f.provenance
        

def test_stage_provenance():
    DS = PipelineStage.data_store
    DS.clear()
    
    datapath = os.path.join(DATADIR, 'testdata', 'test_dc2_training_9816.pq')
    cm = ColumnMapper.make_stage(input=datapath, chunk_size=1000,
                                 hdf5_groupname='photometry', columns=dict(id='bob'))

    prov = cm.provenance
    assert prov == cm.provenance

    th = Hdf5Handle('data', path=datapath)

    out_handle = cm(th)
    cm.finalize()

    pc = ProvChecker.make_stage(input=out_handle.path)
    assert pc.connect_input(cm, 'input', 'output') is not None
    assert pc.connect_input(cm) is not None
    pc.run()
    
    
    
def test_stage_interface():
    DS = PipelineStage.data_store
    DS.clear()
    
    datapath = os.path.join(DATADIR, 'testdata', 'test_dc2_training_9816.pq')
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
    
    datapath = os.path.join(DATADIR, 'testdata', 'test_dc2_training_9816.hdf5')
    cm = ColumnMapper.make_stage(input=datapath, chunk_size=1000,
                                 hdf5_groupname='photometry', columns=dict(id='bob'))
    x = cm.input_iterator('input')

    assert isinstance(x, GeneratorType)

    for i, xx in enumerate(x):
        assert xx[0] == i*1000
        assert xx[1] - xx[0] <= 1000

