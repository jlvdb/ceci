from parsl import clear
import tempfile
import os
import pytest
import subprocess

from ceci.pipeline import Pipeline
from ceci_example.example_stages import *
from ceci.config import StageConfig


def test_config():
    config_options = {'chunk_rows': 5000, 'something':float, 'free':None}
    config = StageConfig(**config_options)

    assert config['chunk_rows'] == 5000
    assert config.chunk_rows == 5000
    assert getattr(config, 'chunk_rows') == 5000

    config.chunk_rows = 133
    assert config.chunk_rows == 133

    config.free = 'dog'
    config.free = 42

    try:
        config.chunk_rows = 'a'
    except TypeError:
        pass
    else:
        raise RuntimeError("Failed to catch a type error")

    try:
        config['chunk_rows'] = 'a'
    except TypeError:
        pass
    else:
        raise RuntimeError("Failed to catch a type error")
    assert config.chunk_rows == 133

    config['new_par'] = 'abc'
    assert config['new_par'] == 'abc'
    assert config.get_type('new_par') == str

    config.reset()
    assert config.chunk_rows == 5000

    assert config.get_type('chunk_rows') == int

    values = config.values()
    for key, value in config.items():
        #assert value == config[key].value
        assert value in values

    def check_func(cfg, **kwargs):
        for k, v in kwargs.items():
            check_type = cfg.get_type(k)
            if k is not None and v is not None:
                assert check_type == type(v)

    check_func(config, **config)

    for k in iter(config):
        assert k in config

    
    

def test_interactive_pipeline():

    # Load the pipeline interactively, this is just a temp fix to
    # get the run_config and stage_config
    pipeline = Pipeline.read('tests/test.yml')
    # pipeline.run()

    dry_pipe = Pipeline.read('tests/test.yml', dry_run=True)
    dry_pipe.run()

    pipe2 = Pipeline.interactive()
    overall_inputs = {'DM':'./tests/inputs/dm.txt',
                      'fiducial_cosmology':'./tests/inputs/fiducial_cosmology.txt'}
    inputs = overall_inputs.copy()
    inputs['metacalibration'] = True
    inputs['config'] = None

    pipe2.pipeline_files.update(**inputs)
    pipe2.build_stage(PZEstimationPipe)
    pipe2.build_stage(shearMeasurementPipe, apply_flag=False)
    pipe2.build_stage(WLGCSelector, zbin_edges=[0.2, 0.3, 0.5], ra_range=[-5, 5])
    pipe2.build_stage(SysMapMaker)
    pipe2.build_stage(SourceSummarizer)
    pipe2.build_stage(WLGCCov, covariance='covariance_copy')
    pipe2.build_stage(WLGCRandoms)
    pipe2.build_stage(WLGCTwoPoint, name="WLGC2Point")
    pipe2.build_stage(WLGCSummaryStatistic, covariance='covariance_copy')

    assert len(pipe2.WLGCCov.outputs) == 1

    pipe2.initialize(overall_inputs, pipeline.run_config, pipeline.stages_config)

    pipe2.print_stages()
    pipe2.WLGCCov.print_io()

    assert pipe2['WLGCCov'] == pipe2.WLGCCov

    rpr = repr(pipe2.WLGCCov.config)

    path = pipe2.pipeline_files.get_path('covariance')
    assert pipe2.pipeline_files.get_tag(path) == 'covariance'
    assert pipe2.pipeline_files.get_type('covariance') == pipe2.WLGCCov.get_output_type('covariance')

    pipe2.run()



def test_inter_pipe():

    pipe2 = Pipeline.interactive()
    overall_inputs = {'DM':'./tests/inputs/dm.txt',
                      'fiducial_cosmology':'./tests/inputs/fiducial_cosmology.txt'}
    inputs = overall_inputs.copy()
    inputs['config'] = None

    pipe2.pipeline_files.update(**inputs)

    pipe2.build_stage(PZEstimationPipe, name='bob')
    assert isinstance(pipe2.bob, PZEstimationPipe)
    pipe2.remove_stage('bob')

    assert not hasattr(pipe2, 'bob')


def test_build_interactive_pipe():

    pipe = Pipeline.interactive()

    global_config = dict(metacalibration=True)
    overall_inputs = {'DM':'./tests/inputs/dm.txt',
                      'fiducial_cosmology':'./tests/inputs/fiducial_cosmology.txt'}

    pipe.PZEstimationPipe = PZEstimationPipe.build()
    pipe.shearMeasurementPipe = shearMeasurementPipe.build(
        apply_flag=False,
        **global_config,
    )
    pipe.WLGCSelector = WLGCSelector.build(
        connections=dict(
            shear_catalog=pipe.shearMeasurementPipe.io.shear_catalog,
            photoz_pdfs=pipe.PZEstimationPipe.io.photoz_pdfs,
        ),
        zbin_edges=[0.2, 0.3, 0.5],
        ra_range=[-5, 5],
        **global_config,
    )
    pipe.SysMapMaker = SysMapMaker.build()
    pipe.SourceSummarizer = SourceSummarizer.build(
        connections=dict(
            tomography_catalog=pipe.WLGCSelector.io.tomography_catalog,
            photoz_pdfs=pipe.PZEstimationPipe.io.photoz_pdfs,
        ),
        **global_config,
    )
    pipe.WLGCCov = WLGCCov.build(
        connections=dict(
            tomography_catalog=pipe.WLGCSelector.io.tomography_catalog,
            shear_catalog=pipe.shearMeasurementPipe.io.shear_catalog,
            source_summary_data=pipe.SourceSummarizer.io.source_summary_data,
            diagnostic_maps=pipe.SysMapMaker.io.diagnostic_maps,
        ),
        **global_config,
    )
    pipe.WLGCRandoms = WLGCRandoms.build()
    pipe.WLGCTwoPoint = WLGCTwoPoint.build(
        connections=dict(
            tomography_catalog=pipe.WLGCSelector.io.tomography_catalog,
            shear_catalog=pipe.shearMeasurementPipe.io.shear_catalog,
            diagnostic_maps=pipe.SysMapMaker.io.diagnostic_maps,
            random_catalog=pipe.WLGCRandoms.io.random_catalog,
        ),
        **global_config,
    )
    pipe.WLGCSummaryStatistic = WLGCSummaryStatistic.build(
        connections=dict(
            twopoint_data=pipe.WLGCTwoPoint.io.twopoint_data,
            source_summary_data=pipe.SourceSummarizer.io.source_summary_data,
        ),        
        **global_config,
    )

    output_dir = 'temp_out'
    run_config = dict(output_dir=output_dir, log_dir=output_dir, resume=False)
    pipe.initialize(overall_inputs, run_config, None)
    pipe.save('test_example.yml')
    os.system(f"\\rm -rf {output_dir}")




if __name__ == "__main__":
    test_config()
    test_interactive()
    test_build_interactive_pipe()
