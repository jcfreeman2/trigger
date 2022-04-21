# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

from pprint import pprint
pprint(moo.io.default_load_path)
# Load configuration types
import moo.otypes

moo.otypes.load_types('trigger/triggerprimitivemaker.jsonnet')
moo.otypes.load_types('trigger/triggerzipper.jsonnet')
moo.otypes.load_types('trigger/faketpcreatorheartbeatmaker.jsonnet')
moo.otypes.load_types('trigger/tpchannelfilter.jsonnet')

# Import new types
import dunedaq.trigger.triggerprimitivemaker as tpm
import dunedaq.trigger.triggerzipper as tzip
import dunedaq.trigger.faketpcreatorheartbeatmaker as ftpchm
import dunedaq.trigger.tpchannelfilter as chan_filter

from appfwk.app import App, ModuleGraph
from appfwk.daqmodule import DAQModule
from appfwk.conf_utils import Direction, Connection

class TestChannelFilterApp(App):
    def __init__(self,
                 INPUT_FILES: [str],
                 SLOWDOWN_FACTOR: float,
                 CHANNEL_MAP_NAME: str,
                 KEEP_COLLECTION: bool,
                 KEEP_INDUCTION: bool):

        clock_frequency_hz = 50_000_000 / SLOWDOWN_FACTOR
        modules = []

        n_streams = len(INPUT_FILES)

        tp_streams = [tpm.TPStream(filename=input_file,
                                   region_id = 0,
                                   element_id = istream,
                                   output_sink_name = f"output{istream}")
                      for istream,input_file in enumerate(INPUT_FILES)]

        tpm_connections = { f"output{istream}" : Connection(f"chan_filter{istream}.tpset_source")
                            for istream in range(n_streams) }
        modules.append(DAQModule(name = "tpm",
                                 plugin = "TriggerPrimitiveMaker",
                                 conf = tpm.ConfParams(tp_streams = tp_streams,
                                                       number_of_loops=-1, # Infinite
                                                       tpset_time_offset=0,
                                                       tpset_time_width=10000,
                                                       clock_frequency_hz=clock_frequency_hz,
                                                       maximum_wait_time_us=1000,),
                                 connections = tpm_connections))

        for istream in range(n_streams):
            modules.append(DAQModule(name = f"chan_filter{istream}",
                                     plugin = "TPChannelFilter",
                                     conf = chan_filter.Conf(channel_map_name=CHANNEL_MAP_NAME,
                                                             keep_collection=KEEP_COLLECTION,
                                                             keep_induction=KEEP_INDUCTION),
                                     connections = {"tpset_sink" : Connection(f"ftpchm{istream}.tpset_source")}))

            modules.append(DAQModule(name = f"ftpchm{istream}",
                                     plugin = "FakeTPCreatorHeartbeatMaker",
                                     conf = ftpchm.Conf(heartbeat_interval = 500_000),
                                     connections = {"tpset_sink" : Connection("zip.input")}))

        modules.append(DAQModule(name = "zip",
                                 plugin = "TPZipper",
                                 conf = tzip.ConfParams(cardinality=n_streams,
                                                        max_latency_ms=10,
                                                        region_id=0,
                                                        element_id=0,),
                                 connections = {"output" : Connection("tps_sink.tpset_source")}))

        modules.append(DAQModule(name = "tps_sink",
                                 plugin = "TPSetSink"))
        
        mgraph = ModuleGraph(modules)
        super().__init__(modulegraph=mgraph, host="localhost", name='FakeTPToSinkApp')
                                 
