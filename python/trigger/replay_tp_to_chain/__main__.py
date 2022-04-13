import json
import os
import rich.traceback
from rich.console import Console

from appfwk.system import System
from appfwk.conf_utils import AppConnection

# Add -h as default help option
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

console = Console()

# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

# Load configuration types
import moo.otypes
moo.otypes.load_types('networkmanager/nwmgr.jsonnet')
import dunedaq.networkmanager.nwmgr as nwmgr

import click

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-s', '--slowdown-factor', default=1.0)
@click.option('-f', '--input-file', type=click.Path(exists=True, dir_okay=False), multiple=True)
@click.option('--trigger-activity-plugin', default='TriggerActivityMakerPrescalePlugin', help="Trigger activity algorithm plugin")
@click.option('--trigger-activity-config', default='dict(prescale=100)', help="Trigger activity algorithm config (string containing python dictionary)")
@click.option('--trigger-candidate-plugin', default='TriggerCandidateMakerPrescalePlugin', help="Trigger candidate algorithm plugin")
@click.option('--trigger-candidate-config', default='dict(prescale=100)', help="Trigger candidate algorithm config (string containing python dictionary)")
@click.argument('json_dir', type=click.Path())
def cli(slowdown_factor, input_file, trigger_activity_plugin, trigger_activity_config, trigger_candidate_plugin, trigger_candidate_config, json_dir):
    """
      JSON_DIR: Json file output folder
    """

    partition_name="replay_tp_partition"
    the_system = System(partition_name)
    
    console.log("Loading faketp config generator")
    from .replay_tp_app import get_replay_app
    from daqconf.apps.dataflow_gen import get_dataflow_app
    from daqconf.apps.trigger_gen import get_trigger_app
    from daqconf.apps.dfo_gen import get_dfo_app
    console.log(f"Generating configs")

    ru_configs=[{"host": "localhost",
                 "card_id": 0,
                 "region_id": 0,
                 "start_channel": 0,
                 "channel_count": len(input_file)}]
    
    the_system.apps["replay"] = get_replay_app(
        INPUT_FILES = input_file,
        SLOWDOWN_FACTOR = slowdown_factor
    )

    the_system.apps['trigger'] = get_trigger_app(
        PARTITION = partition_name,
        SOFTWARE_TPG_ENABLED = True,
        ACTIVITY_PLUGIN = trigger_activity_plugin,
        ACTIVITY_CONFIG = eval(trigger_activity_config),
        CANDIDATE_PLUGIN = trigger_candidate_plugin,
        CANDIDATE_CONFIG = eval(trigger_candidate_config),
        RU_CONFIG = ru_configs,
        HOST="localhost"
    )

    the_system.apps['dfo'] = get_dfo_app(
        DF_COUNT = 1,
        TOKEN_COUNT = 10,
        PARTITION=partition_name,
        HOST="localhost"
    )

    the_system.apps["dataflow0"] = get_dataflow_app(
        HOSTIDX = 0,
        OUTPUT_PATH = ".",
        PARTITION=partition_name,
        HOST="localhost"
    )

    ru_config = ru_configs[0]
    apa_idx = ru_config['region_id']
    for link in range(ru_config["channel_count"]):
        # PL 2022-02-02: global_link is needed here to have non-overlapping app connections if len(ru)>1 with the same region_id
        # Adding the ru number here too, in case we have many region_ids
        global_link = link+ru_config["start_channel"]
        the_system.app_connections.update(
                    {
                        f"replay.tp_output{global_link}":
                        AppConnection(nwmgr_connection=f"{partition_name}.tpsets_apa{apa_idx}_link{global_link}",
                                      msg_type="dunedaq::trigger::TPSet",
                                      msg_module_name="TPSetNQ",
                                      topics=["TPSets"],
                                      receivers=([f"trigger.tpsets_into_buffer_ru0_link{link}",
                                                  f"trigger.tpsets_into_chain_ru0_link{link}"]))
                    })

    the_system.app_connections[f"dfo.trigger_decisions0"] = AppConnection(nwmgr_connection=f"{partition_name}.trigdec_0",
                                                                          msg_type="dunedaq::dfmessages::TriggerDecision",
                                                                          msg_module_name="TriggerDecisionNQ",
                                                                          topics=[],
                                                                          receivers=[f"dataflow0.trigger_decisions"])

    the_system.app_connections["trigger.td_to_dfo"] = AppConnection(nwmgr_connection=f"{partition_name}.td_mlt_to_dfo",
                                                                    topics=[],
                                                                    use_nwqa=False,
                                                                    receivers=["dfo.td_to_dfo"])

    the_system.app_connections["dfo.df_busy_signal"] = AppConnection(nwmgr_connection=f"{partition_name}.df_busy_signal",
                                                                     topics=[],
                                                                     use_nwqa=False,
                                                                     receivers=["trigger.df_busy_signal"])

    the_system.network_endpoints.append(nwmgr.Connection(name=f"{the_system.partition_name}.triginh",
                                                         topics=[],
                                                         address=f"tcp://{{host_dfo}}:{the_system.next_unassigned_port()}"))
    the_system.network_endpoints.append(nwmgr.Connection(name=f"{the_system.partition_name}.trmon_dqm2df_0", topics=[], address=f"tcp://{{host_dataflow0}}:{the_system.next_unassigned_port()}"))
    the_system.network_endpoints.append(nwmgr.Connection(name=f"{the_system.partition_name}.hsievents", topics=[], address=f"tcp://{{host_dataflow0}}:{the_system.next_unassigned_port()}"))
    
    from appfwk.conf_utils import add_network, make_app_command_data
    from daqconf.core.fragment_producers import  connect_all_fragment_producers, set_mlt_links, remove_mlt_link

    connect_all_fragment_producers(the_system)
    set_mlt_links(the_system, "trigger")

    add_network("trigger", the_system)
    add_network("dfo", the_system)
    add_network("replay", the_system)
    add_network("dataflow0", the_system)

    from appfwk.conf_utils import make_app_command_data, make_system_command_datas, generate_boot, write_json_files
    # Arrange per-app command data into the format used by util.write_json_files()
    app_command_datas = {
        name : make_app_command_data(the_system, app)
        for name,app in the_system.apps.items()
    }
    system_command_datas = make_system_command_datas(the_system)
    boot = generate_boot(the_system.apps)
    write_json_files(app_command_datas, system_command_datas, json_dir)

if __name__ == '__main__':

    try:
            cli(show_default=True, standalone_mode=True)
    except Exception as e:
            console.print_exception()
