# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes

moo.otypes.load_types('trigger/triggerprimitivemaker.jsonnet')
moo.otypes.load_types('trigger/triggeractivitymaker.jsonnet')
moo.otypes.load_types('trigger/triggercandidatemaker.jsonnet')
moo.otypes.load_types('trigger/moduleleveltrigger.jsonnet')
moo.otypes.load_types('trigger/fakedataflow.jsonnet')
moo.otypes.load_types('trigger/triggerzipper.jsonnet')
moo.otypes.load_types('trigger/faketpcreatorheartbeatmaker.jsonnet')

# Import new types

import dunedaq.trigger.triggerprimitivemaker as tpm
import dunedaq.trigger.triggeractivitymaker as tam
import dunedaq.trigger.triggercandidatemaker as tcm
import dunedaq.trigger.moduleleveltrigger as mlt
import dunedaq.trigger.fakedataflow as fdf
import dunedaq.trigger.triggerzipper as tzip
import dunedaq.trigger.faketpcreatorheartbeatmaker as ftpchm

#FIXME maybe one day, triggeralgs will define schemas... for now allow a dictionary of 4byte int, 4byte floats, and strings
moo.otypes.make_type(schema='number', dtype='i4', name='temp_integer', path='temptypes')
moo.otypes.make_type(schema='number', dtype='f4', name='temp_float', path='temptypes')
moo.otypes.make_type(schema='string', name='temp_string', path='temptypes')
def make_moo_record(conf_dict,name,path='temptypes'):
    fields = []
    for pname,pvalue in conf_dict.items():
        typename = None
        if type(pvalue) == int:
            typename = 'temptypes.temp_integer'
        elif type(pvalue) == float:
            typename = 'temptypes.temp_float'
        elif type(pvalue) == str:
            typename = 'temptypes.temp_string'
        else:
            raise Exception(f'Invalid config argument type: {type(value)}')
        fields.append(dict(name=pname,item=typename))
    moo.otypes.make_type(schema='record', fields=fields, name=name, path=path)

def generate(
        INPUT_FILES: str,
        SLOWDOWN_FACTOR: float,
        
        ACTIVITY_PLUGIN: str = 'TriggerActivityMakerPrescalePlugin',
        ACTIVITY_CONFIG: dict = dict(prescale=1000),
        
        CANDIDATE_PLUGIN: str = 'TriggerCandidateMakerPrescalePlugin',
        CANDIDATE_CONFIG: int = dict(prescale=1000),
        
        TOKEN_COUNT: int = 10,
        
        FORGET_DECISION_PROB: float = 0.0,
        HOLD_DECISION_PROB: float = 0.0,
        HOLD_MAX_SIZE: int = 0,
        HOLD_MIN_SIZE: int = 0,
        HOLD_MIN_MS: int = 0,
        RELEASE_RANDOMLY_PROB: float = 0.0,

        CLOCK_SPEED_HZ: int = 50000000
):

    # Derived parameters
    CLOCK_FREQUENCY_HZ = CLOCK_SPEED_HZ / SLOWDOWN_FACTOR

    make_moo_record(ACTIVITY_CONFIG,'ActivityConf','temptypes')
    make_moo_record(CANDIDATE_CONFIG,'CandidateConf','temptypes')
    import temptypes

    from ..util import module
    
    modules = {}

    for i,input_file in enumerate(INPUT_FILES):
        modules[f"tpm{i}"] = module(plugin="TriggerPrimitiveMaker",
                                 connections={"tpset_sink": f"ftpchm{i}.tpset_source"},
                                 conf=tpm.ConfParams(filename=input_file,
                                                     number_of_loops=-1, # Infinite
                                                     tpset_time_offset=0,
                                                     tpset_time_width=10000,
                                                     clock_frequency_hz=CLOCK_FREQUENCY_HZ,
                                                     maximum_wait_time_us=1000,
                                                     region_id=0,
                                                     element_id=i))
        modules[f"ftpchm{i}"] = module(plugin="FakeTPCreatorHeartbeatMaker",
                                       connections={"tpset_sink" : "zip.input"},
                                       conf=ftpchm.Conf(heartbeat_interval = 50000))

    modules["zip"] = module(plugin="TPZipper",
                            connections={"output" : "tam.input"},
                            conf=tzip.ConfParams(cardinality=len(INPUT_FILES),
                                                 max_latency_ms=1000,
                                                 region_id=0,
                                                 element_id=0))

    modules["tam"] = module(plugin="TriggerActivityMaker",
                            connections={"output" : "tcm.input"},
                            conf=tam.Conf(activity_maker=ACTIVITY_PLUGIN,
                                          geoid_region=0, # Fake placeholder
                                          geoid_element=0, # Fake placeholder
                                          window_time=10000, # should match whatever makes TPSets, in principle
                                          buffer_time=625000, # 10ms in 62.5 MHz ticks
                                          activity_maker_config=temptypes.ActivityConf(**ACTIVITY_CONFIG)))
    
    modules["tcm"] = module(plugin="TriggerCandidateMaker",
                            connections={"output" : "mlt.trigger_candidate_source"},
                            conf=tcm.Conf(candidate_maker=CANDIDATE_PLUGIN,
                                          candidate_maker_config=temptypes.CandidateConf(**CANDIDATE_CONFIG)))

    modules["mlt"] = module(plugin="ModuleLevelTrigger",
                            connections={"trigger_decision_sink": "fdf.trigger_decision_source"},
                            conf=mlt.ConfParams(links=[],
                                                initial_token_count=TOKEN_COUNT))

    modules["fdf"] = module(plugin="FakeDataFlow",
                            connections={"!trigger_complete_sink": "mlt.token_source"},
                            conf=fdf.ConfParams(hold_max_size = HOLD_MAX_SIZE,
                                                hold_min_size = HOLD_MIN_SIZE,
                                                hold_min_ms = HOLD_MIN_MS,
                                                release_randomly_prob = RELEASE_RANDOMLY_PROB,
                                                forget_decision_prob = FORGET_DECISION_PROB,
                                                hold_decision_prob = HOLD_DECISION_PROB))
    return modules
