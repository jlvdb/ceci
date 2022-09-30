import os
import pytest
import pickle
import numpy as np
import pandas as pd
from types import GeneratorType

import ceci
from ceci.stage import PipelineStage
from ceci.data import DataStore, TableHandle, DataHandle, PqHandle, Hdf5Handle, FitsHandle

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


def do_data_handle(datapath, handle_class):

    DS = PipelineStage.data_store

    th = handle_class('data', path=datapath)

    with pytest.raises(ValueError) as errinfo:
        th.write()

    assert not th.has_data
    with pytest.raises(ValueError) as errinfo:
        th.write_chunk(0, 1)        
    assert th.has_path
    assert th.is_written
    data = th.read()
    data2 = th.read()

    assert data is data2
    assert th.has_data
    assert th.make_name('data') == f'data.{handle_class.suffix}'
    
    th2 = handle_class('data2', data=data)
    assert th2.has_data
    assert not th2.has_path
    assert not th2.is_written
    with pytest.raises(ValueError) as errinfo:
        th2.open()
    with pytest.raises(ValueError) as errinfo:
        th2.write()
    with pytest.raises(ValueError) as errinfo:
        th2.write_chunk(0, 1)
        
    assert th2.make_name('data2') == f'data2.{handle_class.suffix}'
    assert str(th)
    assert str(th2)
    return th
    

def test_pq_handle():
    datapath = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816.pq')
    handle = do_data_handle(datapath, PqHandle)
    pqfile = handle.open()
    assert pqfile
    assert handle.fileObj is not None
    handle.close()
    assert handle.fileObj is None

    
def test_hdf5_handle():
    datapath = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816.hdf5')
    handle = do_data_handle(datapath, Hdf5Handle)
    with handle.open(mode='r') as f:
        assert f
        assert handle.fileObj is not None
    datapath_chunked = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816_chunked.hdf5')
    handle_chunked = Hdf5Handle("chunked", handle.data, path=datapath_chunked)
    from tables_io.arrayUtils import getGroupInputDataLength, sliceDict, getInitializationForODict
    num_rows = len(handle.data['photometry']['id'])
    check_num_rows = len(handle()['photometry']['id'])
    assert num_rows == check_num_rows
    chunk_size = 1000
    data = handle.data['photometry']
    init_dict = getInitializationForODict(data)
    with handle_chunked.open(mode='w') as fout:
        for k, v in init_dict.items():
            fout.create_dataset(k, v[0], v[1])
        for i in range(0, num_rows, chunk_size):
            start = i
            end = i+chunk_size
            if end > num_rows:
                end = num_rows
            handle_chunked.set_data(sliceDict(handle.data['photometry'], slice(start, end)), partial=True)
            handle_chunked.write_chunk(start, end)
    write_size = handle_chunked.size()
    assert len(handle_chunked.data) <= 1000
    data_called = handle_chunked()
    assert len(data_called['id']) == write_size
    read_chunked = Hdf5Handle("read_chunked", None, path=datapath_chunked)
    data_check = read_chunked.read()
    assert np.allclose(data['id'], data_check['id'])
    assert np.allclose(data_called['id'], data_check['id'])
    os.remove(datapath_chunked)


def test_fits_handle():
    datapath = os.path.join(CECIDIR, 'tests', 'data', 'output_BPZ_lite.fits')
    handle = do_data_handle(datapath, FitsHandle)
    fitsfile = handle.open()
    assert fitsfile
    assert handle.fileObj is not None
    handle.close()
    assert handle.fileObj is None


def test_data_hdf5_iter():

    DS = PipelineStage.data_store
    DS.clear()
    
    datapath = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816.hdf5')

    #data = DS.read_file('data', TableHandle, datapath)
    th = Hdf5Handle('data', path=datapath)
    x = th.iterator(groupname='photometry', chunk_size=1000)

    assert isinstance(x, GeneratorType)
    for i, xx in enumerate(x):
        assert xx[0] == i*1000
        assert xx[1] - xx[0] <= 1000

    data = DS.read_file('input', TableHandle, datapath)        
    cm = ColumnMapper.make_stage(input=datapath, chunk_size=1000,
                                 hdf5_groupname='photometry', columns=dict(id='bob'))
    x = cm.input_iterator('input')

    assert isinstance(x, GeneratorType)

    for i, xx in enumerate(x):
        assert xx[0] == i*1000
        assert xx[1] - xx[0] <= 1000


def test_data_store():
    DS = PipelineStage.data_store
    DS.clear()
    DS.__class__.allow_overwrite = False
    datapath_hdf5 = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816.hdf5')
    datapath_pq = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816.pq')
    datapath_hdf5_copy = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816_copy.hdf5')
    datapath_pq_copy = os.path.join(CECIDIR, 'tests', 'data', 'test_dc2_training_9816_copy.pq')

    DS.add_data('hdf5', None, Hdf5Handle, path=datapath_hdf5)
    DS.add_data('pq', None, PqHandle, path=datapath_pq)

    with DS.open('hdf5') as f:
        assert f
    
    data_pq = DS.read('pq')
    data_hdf5 = DS.read('hdf5')

    DS.add_data('pq_copy', data_pq, PqHandle, path=datapath_pq_copy)
    DS.add_data('hdf5_copy', data_hdf5, Hdf5Handle, path=datapath_hdf5_copy)
    DS.write('pq_copy')
    DS.write('hdf5_copy')

    with pytest.raises(KeyError) as errinfo:
        DS.read('nope')
    with pytest.raises(KeyError) as errinfo:
        DS.open('nope')
    with pytest.raises(KeyError) as errinfo:
        DS.write('nope')

    with pytest.raises(TypeError) as errinfo:
        DS['nope'] = None
    with pytest.raises(ValueError) as errinfo:        
        DS['pq'] = DS['pq']        
    with pytest.raises(ValueError) as errinfo:
        DS.pq = DS['pq']

    assert repr(DS) 

    DS2 = DataStore(pq=DS.pq)
    assert isinstance(DS2.pq, DataHandle)

    # pop the 'pq' data item to avoid overwriting file under git control
    DS.pop('pq')
    
    DS.write_all()
    DS.write_all(force=True)
    
    os.remove(datapath_hdf5_copy)
    os.remove(datapath_pq_copy)

