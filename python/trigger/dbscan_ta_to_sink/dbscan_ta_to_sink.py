# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes

moo.otypes.load_types('trigger/triggerprimitivemaker.jsonnet')
moo.otypes.load_types('trigger/triggeractivitymaker.jsonnet')
moo.otypes.load_types('trigger/triggerzipper.jsonnet')
moo.otypes.load_types('trigger/faketpcreatorheartbeatmaker.jsonnet')
moo.otypes.load_types('trigger/tasetsink.jsonnet')

# Import new types
import dunedaq.trigger.triggerprimitivemaker as tpm
import dunedaq.trigger.triggeractivitymaker as tam
import dunedaq.trigger.triggerzipper as tzip
import dunedaq.trigger.faketpcreatorheartbeatmaker as ftpchm
import dunedaq.trigger.tasetsink as tasetsink

from appfwk.app import App, ModuleGraph
from appfwk.daqmodule import DAQModule
from appfwk.conf_utils import Direction, Connection

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

#===============================================================================
def generate(
        INPUT_FILES: str,
        OUTPUT_FILE: str,
        SLOWDOWN_FACTOR: float,
        NUMBER_OF_LOOPS: int,
        ACTIVITY_CONFIG: dict = dict(min_pts=3),
):
    # Derived parameters
    CLOCK_FREQUENCY_HZ = 50_000_000 / SLOWDOWN_FACTOR
    TPSET_WIDTH = 10_000
    
    modules = []

    # mod_specs = [
    #     mspec(f"tpm{i}", "TriggerPrimitiveMaker", [
    #         app.QueueInfo(name="tpset_sink", inst=f"tpset_q", dir="output"),
    #     ]) for i in range(len(INPUT_FILES))
    # ] + [
    #     mspec("zip", "TPZipper", [
    #         app.QueueInfo(name="input", inst="tpset_q", dir="input"),
    #         app.QueueInfo(name="output", inst="zipped_tpset_q", dir="output"),
    #     ]),
        
    #     mspec('tam', 'TriggerActivityMaker', [ # TPSet -> TASet
    #         app.QueueInfo(name='input', inst='zipped_tpset_q', dir='input'),
    #         app.QueueInfo(name='output', inst='taset_q', dir='output'),
    #     ]),

    #     mspec("ta_sink", "TASetSink", [
    #         app.QueueInfo(name="taset_source", inst="taset_q", dir="input"),
    #     ]),
    # ]

    
    make_moo_record(ACTIVITY_CONFIG,'ActivityConf','temptypes')
    import temptypes

    
    for i,input_file in enumerate(INPUT_FILES):
        modules.append(DAQModule(name = f"tpm{i}",
                                 plugin = "TriggerPrimitiveMaker",
                                 connections = {"tpset_sink": Connection(f"ftpchm{i}.tpset_source")},
                                 conf = tpm.ConfParams(filename=input_file,
                                                       number_of_loops=NUMBER_OF_LOOPS,
                                                       tpset_time_offset=0,
                                                       tpset_time_width=10000,
                                                       clock_frequency_hz=CLOCK_FREQUENCY_HZ,
                                                       maximum_wait_time_us=1000,
                                                       region_id=0,
                                                       element_id=i)))

        modules.append(DAQModule(name = f"ftpchm{i}",
                                 plugin = "FakeTPCreatorHeartbeatMaker",
                                 connections = {"tpset_sink": Connection("zip.input")},
                                 conf = ftpchm.Conf(heartbeat_interval = 50000)))

    modules.append(DAQModule(name = "zip",
                             plugin = "TPZipper",
                             connections = {"output": Connection("tam.input")},
                             conf = tzip.ConfParams(cardinality=len(INPUT_FILES),
                                                    max_latency_ms=100,
                                                    region_id=0,
                                                    element_id=0)))
    modules.append(DAQModule(name = "tam",
                             plugin = "TriggerActivityMaker",
                             connections = {"output": Connection("ta_sink.taset_source")},
                             conf = tam.Conf(activity_maker="TriggerActivityMakerDBSCANPlugin",
                                             geoid_region=0, # Fake placeholder
                                             geoid_element=0, # Fake placeholder
                                             window_time=10000, # should match whatever makes TPSets, in principle
                                             buffer_time=625000, # 10ms in 62.5 MHz ticks
                                             activity_maker_config=temptypes.ActivityConf(**ACTIVITY_CONFIG))))
    modules.append(DAQModule(name = "ta_sink",
                             plugin = "TASetSink",
                             connections = {},
                             conf = tasetsink.Conf(output_filename=OUTPUT_FILE)))

    mgraph = ModuleGraph(modules)
    return App(modulegraph = mgraph, host="localhost", name="TASinkApp")
