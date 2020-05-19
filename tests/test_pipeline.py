from ceci import PipelineStage, MiniPipeline, ParslPipeline, Pipeline
from ceci_example.types import TextFile
from ceci.sites import load
import pytest
from parsl import clear

# This one should work
class AAA(PipelineStage):
    name = "AAA"
    inputs = [('b', TextFile)]
    outputs = [('a', TextFile)]
    config = {}

class BBB(PipelineStage):
    name = "BBB"
    inputs = [('a', TextFile)]
    outputs = [('b', TextFile)]
    config = {}

class CCC(PipelineStage):
    name = "CCC"
    inputs = [('b', TextFile)]
    outputs = [('c', TextFile)]
    config = {}

class DDD(PipelineStage):
    name = "DDD"
    inputs = [('b', TextFile), ('c', TextFile)]
    outputs = [('d', TextFile)]
    config = {}

def test_orderings():


    # TODO: make it so less boilerplate is needed here
    launcher_config = {'interval': 0.5, 'name':'mini'}
    site = load(launcher_config, [{'name': 'local'}])[0]

    A = {'name': 'AAA', 'site':site}
    B = {'name': 'BBB', 'site':site}
    C = {'name': 'CCC', 'site':site}
    D = {'name': 'DDD', 'site':site}

        
    # This one should work - basic pipeline
    # as long as we supply input 'a'.
    # order should be A then C
    pipeline = Pipeline([C,B], launcher_config)
    order = pipeline.ordered_stages({'a': 'a.txt'})
    assert [s.name for s in order] == ['BBB', 'CCC']

    pipeline = Pipeline([C, D, B], launcher_config)
    order = pipeline.ordered_stages({'a': 'a.txt'})
    assert [s.name for s in order] == ['BBB', 'CCC', 'DDD']

    # Should fail - missing an input, 'a'
    with pytest.raises(ValueError):
        pipeline = Pipeline([D, C, B], launcher_config)
        order = pipeline.ordered_stages({})

    # Should fail - circular
    with pytest.raises(ValueError):
        pipeline = Pipeline([A, B], launcher_config)
        order = pipeline.ordered_stages({})

    # Should fail - one output is supplied as an input
    # with pytest.raises(ValueError):
    with pytest.raises(ValueError):
        pipeline = Pipeline([A], launcher_config)
        order = pipeline.ordered_stages({'a': 'a.txt', 'b': 'b.txt'})

    # Should fail - repeated stage
    with pytest.raises(ValueError):
        pipeline = Pipeline([A, A], launcher_config)
        order = pipeline.ordered_stages({'b': 'b.txt'})


class FailingStage(PipelineStage):
    name = "FailingStage"
    inputs = []
    outputs = [('dm', TextFile)]  # exists already as an input
    config = {}

    def run(self):
        raise ValueError("This should not run because its outputs exist.")


def _return_value_test_(resume):
    expected_status = 0 if resume else 1
    # Mini pipeline should not run
    launcher_config = {'interval': 0.5, 'name':'mini'}
    site = load(launcher_config, [{'name': 'local'}])[0]
    stage = {'name': 'FailingStage', 'site':site}
    pipeline = MiniPipeline([stage], launcher_config)
    status = pipeline.run({}, './tests/inputs', './tests/logs', resume, 'tests/config.yml')
    assert status == expected_status

    # Parsl pipeline should not run stage either
    launcher_config = {'name':'parsl'}
    site = load(launcher_config, [{'name': 'local', 'max_threads':2}])[0]
    stage = {'name': 'FailingStage', 'site':site}
    pipeline = ParslPipeline([stage], launcher_config)
    status = pipeline.run({}, './tests/inputs', './tests/logs', resume, 'tests/config.yml')
    assert status == expected_status
    clear()


def test_resume():
    _return_value_test_(True)

def test_fail():
    _return_value_test_(False)
