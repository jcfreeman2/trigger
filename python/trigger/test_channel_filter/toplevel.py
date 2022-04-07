import json
import os
import rich.traceback
from rich.console import Console

from appfwk.system import System

# Add -h as default help option
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

console = Console()

import click

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-s', '--slowdown-factor', default=1.0)
@click.option('-f', '--input-file', type=click.Path(exists=True, dir_okay=False), multiple=True)
@click.option('--keep-collection/--discard-collection', is_flag=True, default=True, show_default=True, help="Keep/discard collection TPs")
@click.option('--keep-induction/--discard-induction', is_flag=True, default=True, show_default=True, help="Keep/discard induction TPs")
@click.option('--channel-map-name', type=click.Choice(["VDColdboxChannelMap", "ProtoDUNESP1ChannelMap"]), default="ProtoDUNESP1ChannelMap", help="Channel map name")
@click.argument('json_dir', type=click.Path())
def cli(slowdown_factor, input_file, keep_collection, keep_induction, channel_map_name, json_dir):
    """
      JSON_DIR: Json file output folder
    """

    the_system = System("test_channel_filter_partition")
    
    console.log("Loading faketp config generator")
    from .test_channel_filter import TestChannelFilterApp
    console.log(f"Generating configs")

    the_system.apps["test_channel_filter"] = TestChannelFilterApp(
        INPUT_FILES = input_file,
        SLOWDOWN_FACTOR = slowdown_factor,
        KEEP_COLLECTION = keep_collection,
        KEEP_INDUCTION = keep_induction,
        CHANNEL_MAP_NAME = channel_map_name
    )

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
