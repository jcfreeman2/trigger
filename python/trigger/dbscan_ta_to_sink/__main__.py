import json
import os
import rich.traceback
from rich.console import Console
from os.path import exists, join

from appfwk.system import System

# Add -h as default help option
CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

console = Console()

import click

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-s', '--slowdown-factor', default=1.0)
@click.option('-f', '--input-file', type=click.Path(), multiple=True)
@click.option('-o', '--output-file', type=click.Path())
@click.option('-l', '--number-of-loops', default=1000000)
@click.option('-c', '--do-taset-checks', is_flag=True)
@click.argument('json_dir', type=click.Path())
def cli(slowdown_factor, input_file, output_file, number_of_loops, do_taset_checks, json_dir):
    """
      JSON_DIR: Json file output folder
    """

    partition_name = "ta_sink_partition"
    the_system = System(partition_name)
    
    console.log("Loading faketp config generator")
    from . import dbscan_ta_to_sink
    console.log(f"Generating configs")

    the_system.apps["tasinkapp"] = dbscan_ta_to_sink.generate(
        INPUT_FILES = input_file,
        OUTPUT_FILE = output_file,
        SLOWDOWN_FACTOR = slowdown_factor,
        NUMBER_OF_LOOPS = number_of_loops,
        DO_TASET_CHECKS = do_taset_checks
    )

    from appfwk.conf_utils import make_app_command_data, make_system_command_datas, generate_boot, write_json_files

    app_command_datas = {
        name : make_app_command_data(the_system, app)
        for name,app in the_system.apps.items()
    }


    system_command_datas = make_system_command_datas(the_system)
    # Override the default boot.json with the one from minidaqapp
    boot = generate_boot(the_system.apps, partition_name=partition_name)

    system_command_datas['boot'] = boot

    write_json_files(app_command_datas, system_command_datas, json_dir)



if __name__ == '__main__':

    try:
            cli(show_default=True, standalone_mode=True)
    except Exception as e:
            console.print_exception()
