# Set moo schema search path
from dunedaq.env import get_moo_model_path
import moo.io
moo.io.default_load_path = get_moo_model_path()

from os.path import exists, join
from rich.console import Console
from copy import deepcopy
from collections import namedtuple, defaultdict
import json
import os
from enum import Enum

import moo.otypes

moo.otypes.load_types('rcif/cmd.jsonnet')
moo.otypes.load_types('appfwk/cmd.jsonnet')
moo.otypes.load_types('appfwk/app.jsonnet')

moo.otypes.load_types('nwqueueadapters/networktoqueue.jsonnet')
moo.otypes.load_types('nwqueueadapters/queuetonetwork.jsonnet')

from appfwk.utils import acmd, mcmd, mspec
import dunedaq.nwqueueadapters.networkobjectsender as nos
import dunedaq.nwqueueadapters.queuetonetwork as qton
import dunedaq.nwqueueadapters.networkobjectreceiver as nor
import dunedaq.nwqueueadapters.networktoqueue as ntoq
import dunedaq.appfwk.app as appfwk  # AddressedCmd,
import dunedaq.rcif.cmd as rccmd  # AddressedCmd,

console = Console()

########################################################################
#
# Classes

# TODO: Connections between modules are held in the module object, but
# connections between applications are held in their own
# structure. Not clear yet which is better, but should probably be
# consistent either way

# TODO: Understand whether extra_commands is actually needed. Seems like "resume" is already being sent to everyone?
#module = namedtuple("module", ['plugin', 'conf', 'connections', 'extra_commands'], defaults=(None, [], []))

class module:
    """An individual DAQModule within an application, along with its
       configuration object and list of outgoing connections to other
       modules
    """
    
    def __init__(self, plugin, conf=None, connections=[]):
        self.plugin=plugin
        self.conf=conf
        self.connections=connections

    def __repr__(self):
        return f"module(plugin={self.plugin}, conf={self.conf}, connections={self.connections})"

    def __rich_repr__(self):
        yield "plugin", self.plugin
        yield "conf", self.conf
        yield "connections", self.connections

connection = namedtuple("connection", ['to', 'queue_kind', 'queue_capacity', 'toposort'], defaults=("FollyMPMCQueue", 1000, True))

class direction(Enum):
    IN = 1
    OUT = 2

endpoint = namedtuple("endpoint", [ 'external_name', 'internal_name', 'direction' ])

class modulegraph:
    """A set of modules and connections between them.

    modulegraph holds a dictionary of modules, with each module
    knowing its (outgoing) connections to other modules in the
    modulegraph.

    Connections to other modulegraphs (which may be in the same
    application or a different application) are represented by
    `endpoints`. The endpoint's `external_name` is the "public" name
    of the connection, which other modulegraphs should use. The
    endpoint's `internal_name` specifies the particular module and
    sink/source name which the endpoint is connected to, which may be
    changed without affecting other applications.

    """
    def __init__(self, modules=None, endpoints=None):
        self.modules=modules if modules else dict()
        self.endpoints=endpoints if endpoints else dict()

    def __repr__(self):
        return f"modulegraph(modules={self.modules}, endpoints={self.endpoints})"

    def __rich_repr__(self):
        yield "modules", self.modules
        yield "endpoints", self.endpoints

    def set_from_dict(self, module_dict):
        self.modules=module_dict

    def module_names(self):
        return self.modules.keys()

    def module_list(self):
        return self.modules.values()

    def add_module(self, name, **kwargs):
        mod=module(**kwargs)
        self.modules[name]=mod
        return mod

    def add_connection(self, from_endpoint, to_endpoint):
        from_mod, from_name=from_endpoint.split(".")
        self.modules[from_mod].connections[from_name]=connection(to_endpoint)

    def add_endpoint(self, external_name, internal_name, inout):
        self.endpoints[external_name]=endpoint(external_name, internal_name, inout)

    def endpoint_names(self, inout=None):
        if inout is not None:
            return [ e[0] for e in self.endpoints.items() if e[1].inout==inout ]
        return self.endpoints.keys()


app = namedtuple("app", ['modulegraph', 'host'], defaults=({}, "localhost"))

publisher = namedtuple(
    "publisher", ['msg_type', 'msg_module_name', 'subscribers'])

sender = namedtuple("sender", ['msg_type', 'msg_module_name', 'receiver'])

class system:
    """A full DAQ system consisting of multiple applications and the
    connections between them. The `apps` member is a dictionary from
    application name to app object, and the app_connections member is
    a dictionary from upstream endpoint to publisher or sender object
    representing the downstream endpoint(s). Endpoints are specified
    as strings like app_name.endpoint_name.

    An explicit mapping from upstream endpoint name to zeromq
    connection string may be specified, but typical usage is to not
    specify this, and leave the mapping to be automatically generated.

    The same is true for application start order.

    """
    def __init__(self, apps=None, app_connections=None, network_endpoints=None, app_start_order=None):
        self.apps=apps if apps else dict()
        self.app_connections=app_connections if app_connections else dict()
        self.network_endpoints=network_endpoints
        self.app_start_order=app_start_order

########################################################################
#
# Functions


def make_module_deps(module_dict):
    """Given a dictionary of `module` objects, produce a dictionary giving
    the dependencies between them. A dependency is any connection between
    modules (implemented using an appfwk queue). Connections whose
    upstream ends begin with a '!' are not considered dependencies, to
    allow us to break cycles in the DAG.

    Returns a dictionary where each key is a module name, and the
    corresponding value is a list of names of modules connected "downstream" of
    that module.

    """

    deps = {}
    for name, mod in module_dict.items():
        deps[name] = []
        for upstream_name, downstream_connection in mod.connections.items():
            if downstream_connection.toposort:
                other_mod = downstream_connection.to.split(".")[0]
                deps[name].append(other_mod)
    return deps


def make_app_deps(the_system, verbose=False):
    """Produce a dictionary giving
    the dependencies between a set of applications, given their connections.

    Returns a dictionary analogous to the return value of
    make_module_deps: each key is an application name, and the
    corresponding value is a list of names of applications connected
    "downstream" of that app.

    """

    deps = {}
    for app in the_system.apps.keys():
        deps[app] = []

    for from_endpoint, conn in the_system.app_connections.items():
        from_app = from_endpoint.split(".")[0]
        if hasattr(conn, "subscribers"):
            deps[from_app] += [ds.split(".")[0] for ds in conn.subscribers]
        elif hasattr(conn, "receiver"):
            deps[from_app].append(conn.receiver.split(".")[0])
    return deps


def toposort(deps_orig):
    """Perform a topological sort of a dependency dictionary as produced
    by make_module_deps or make_app_deps. Throws ValueError if a cycle is
    found in the graph"""

    # Kahn's algorithm for topological sort, from Wikipedia:

    # L <- Empty list that will contain the sorted elements
    # S <- Set of all nodes with no incoming edge

    # while S is not empty do
    #     remove a node n from S
    #     add n to L
    #     for each node m with an edge e from n to m do
    #         remove edge e from the graph
    #         if m has no other incoming edges then
    #             insert m into S

    # if graph has edges then
    #     return error   (graph has at least one cycle)
    # else
    #     return L   (a topologically sorted order)

    deps = deepcopy(deps_orig)
    L = []
    S = set([name for name, d in deps.items() if d == []])
    # console.log("Initial nodes with no incoming edges:")
    # console.log(S)

    while S:
        n = S.pop()
        # console.log(f"Popped item {n} from S")
        L.append(n)
        # console.log(f"List so far is {L}")
        tmp = [name for name, d in deps.items() if n in d]
        # console.log(f"Nodes with edge from n: {tmp}")
        for m in tmp:
            # console.log(f"Removing {n} from deps[{m}]")
            deps[m].remove(n)
            if deps[m] == []:
                S.add(m)

    # Are there any cycles?
    if any(deps.values()):
        # TODO: Give some more helpful output about the cycles that exist
        raise ValueError(deps)

    return L


def make_app_command_data(app, verbose=False):
    """Convert a dictionary of `module`s into 'command data' suitable for
    feeding to nanorc. The needed queues are inferred from from
    connections between modules, as are the start and stop order of the
    modules"""

    modules = app.modulegraph.modules

    module_deps = make_module_deps(modules)
    if verbose:
        console.log(f"inter-module dependencies are: {module_deps}")

    start_order = toposort(module_deps)
    stop_order = start_order[::-1]

    if verbose:
        console.log(f"Inferred module start order is {start_order}")
        console.log(f"Inferred module stop order is {stop_order}")

    command_data = {}

    queue_specs = []

    app_qinfos = defaultdict(list)

    # Infer the queues we need based on the connections between modules

    # Terminology: an "endpoint" is "module.name"
    for name, mod in modules.items():
        for from_name, downstream_connection in mod.connections.items():
            # The name might be prefixed with a "!" to indicate that it doesn't participate in dependencies. Remove that here because "!" is illegal in actual queue names
            from_name = from_name.replace("!", "")
            from_endpoint = ".".join([name, from_name])
            to_endpoint=downstream_connection.to
            to_mod, to_name = to_endpoint.split(".")
            queue_inst = f"{from_endpoint}_to_{to_endpoint}".replace(".", "")
            # Is there already a queue connecting either endpoint? If so, we reuse it

            # TODO: This is a bit complicated. Might be nicer to find
            # the list of necessary queues in a first step, and then
            # actually make the QueueSpec/QueueInfo objects
            found_from = False
            found_to = False
            for k, v in app_qinfos.items():
                for qi in v:
                    test_endpoint = ".".join([k, qi.name])
                    if test_endpoint == from_endpoint:
                        found_from = True
                        queue_inst = qi.inst
                    if test_endpoint == to_endpoint:
                        found_to = True
                        queue_inst = qi.inst

            if not (found_from or found_to):
                if verbose:
                    console.log(f"Creating {downstream_connection.queue_kind}({downstream_connection.queue_capacity}) queue with name {queue_inst} connecting {from_endpoint} to {to_endpoint}")
                queue_specs.append(appfwk.QueueSpec(
                    inst=queue_inst, kind=downstream_connection.queue_kind, capacity=downstream_connection.queue_capacity))

            if not found_from:
                if verbose:
                    console.log(f"Adding output queue to module {name}: inst={queue_inst}, name={from_name}")
                app_qinfos[name].append(appfwk.QueueInfo(
                    name=from_name, inst=queue_inst, dir="output"))
            if not found_to:
                if verbose:
                    console.log(f"Adding input queue to module {to_mod}: inst={queue_inst}, name={to_name}")
                app_qinfos[to_mod].append(appfwk.QueueInfo(
                    name=to_name, inst=queue_inst, dir="input"))

    mod_specs = [mspec(name, mod.plugin, app_qinfos[name])
                 for name, mod in modules.items()]

    # Fill in the "standard" command entries in the command_data structure

    command_data['init'] = appfwk.Init(queues=queue_specs, modules=mod_specs)

    # TODO: Conf ordering
    command_data['conf'] = acmd([
        (name, mod.conf) for name, mod in modules.items()
    ])

    startpars = rccmd.StartParams(run=1, disable_data_storage=False)

    command_data['start'] = acmd([(name, startpars) for name in start_order])
    command_data['stop'] = acmd([(name, None) for name in stop_order])
    command_data['scrap'] = acmd([(name, None) for name in stop_order])

    # Optional commands

    # TODO: What does an empty "acmd" actually imply? Does the command get sent to everyone, or no-one?
    command_data['pause'] = acmd([])
    command_data['resume'] = acmd([])

    # TODO: handle modules' `extra_commands`, including "record"

    return command_data


def assign_network_endpoints(the_system, verbose=False):
    """Given a set of applications and connections between them, come up
    with a list of suitable zeromq endpoints. Return value is a mapping
    from name of upstream end of connection to endpoint name.

    Algorithm is to make an endpoint name like tcp://host:port, where
    host is the hostname for the app at the upstream end of the
    connection, port starts at some fixed value, and increases by 1
    for each new endpoint on that host.

    """
    endpoints = {}
    host_ports = defaultdict(int)
    first_port = 12345
    for conn in the_system.app_connections.keys():
        app = conn.split(".")[0]
        host = the_system.apps[app].host
        if host == "localhost":
            host = "127.0.0.1"
        port = first_port + host_ports[host]
        host_ports[host] += 1
        endpoints[conn] = f"tcp://{host}:{port}"
        if verbose:
            console.log(f"Assigned endpoint {endpoints[conn]} for connection {conn}")
    return endpoints


def resolve_endpoint(app, external_name, inout, verbose=False):
    if external_name in app.modulegraph.endpoints:
        e=app.modulegraph.endpoints[external_name]
        if e.direction==inout:
            if verbose:
                console.log(f"Endpoint {external_name} resolves to {e.internal_name}")
            return e.internal_name
        else:
            raise KeyError(f"Endpoint {external_name} has direction {e.direction}, but requested direction was {inout}")
    else:
        raise KeyError(f"Endpoint {external_name} not found")

def add_network(app_name, the_system, verbose=False):

    if the_system.network_endpoints is None:
        the_system.network_endpoints=assign_network_endpoints(the_system)

    app = the_system.apps[app_name]

    modules_with_network = deepcopy(app.modulegraph.modules)

    unconnected_endpoints = set(app.modulegraph.endpoints.keys())

    for conn_name, conn in the_system.app_connections.items():
        from_app, from_endpoint = conn_name.split(".", maxsplit=1)

        if from_app == app_name:
            unconnected_endpoints.remove(from_endpoint)
            from_endpoint = resolve_endpoint(app, from_endpoint, direction.OUT)

            # We're a publisher or sender. Make the queue to network
            qton_name = conn_name.replace(".", "_")
            if verbose:
                console.log(f"Adding QueueToNetwork named {qton_name} connected to {from_endpoint} in app {app_name}")
            modules_with_network[qton_name] = module(plugin="QueueToNetwork",
                                                     connections={
                                                         "input": connection(from_endpoint)},
                                                     conf=qton.Conf(msg_type=conn.msg_type,
                                                                    msg_module_name=conn.msg_module_name,
                                                                    sender_config=nos.Conf(ipm_plugin_type="ZmqPublisher" if type(conn) == publisher else "ZmqSender",
                                                                                           address=the_system.network_endpoints[conn_name],
                                                                                           topic="foo",
                                                                                           stype="msgpack")))
        if hasattr(conn, "subscribers"):
            for to_conn in conn.subscribers:
                to_app, to_endpoint = to_conn.split(".", maxsplit=1)

                if app_name == to_app:
                    unconnected_endpoints.remove(to_endpoint)
                    to_endpoint = resolve_endpoint(app, to_endpoint, direction.IN)
                    ntoq_name = to_conn.replace(".", "_")
                    if verbose:
                        console.log(f"Adding NetworkToQueue named {ntoq_name} connected to {to_endpoint} in app {app_name}")

                    modules_with_network[ntoq_name] = module(plugin="NetworkToQueue",
                                                             connections={
                                                                 "output": connection(to_endpoint)},
                                                             conf=ntoq.Conf(msg_type=conn.msg_type,
                                                                            msg_module_name=conn.msg_module_name,
                                                                            receiver_config=nor.Conf(ipm_plugin_type="ZmqSubscriber",
                                                                                                     address=the_system.network_endpoints[
                                                                                                         conn_name],
                                                                                                     subscriptions=["foo"])))

        if hasattr(conn, "receiver") and app_name == conn.receiver.split(".")[0]:
            # We're a receiver. Add a NetworkToQueue of receiver type
            #
            # TODO: DRY
            unconnected_endpoints.remove(to_endpoint)
            to_endpoint = resolve_endpoint(app, to_endpoint, direction.IN)
            ntoq_name = to_conn.replace(".", "_")
            if verbose:
                console.log(f"Adding NetworkToQueue named {ntoq_name} connected to {to_endpoint} in app {app_name}")
            modules_with_network[ntoq_name] = module(plugin="NetworkToQueue",
                                                     connections={
                                                         "output": connection(to_endpoint)},
                                                     conf=ntoq.Conf(msg_type=conn.msg_type,
                                                                    msg_module_name=conn.msg_module_name,
                                                                    receiver_config=nor.Conf(ipm_plugin_type="ZmqSubscriber",
                                                                                             address=the_system.network_endpoints[conn_name],
                                                                                             subscriptions=["foo"]))
                                                     )

    if unconnected_endpoints:
        # TODO: Use proper logging
        console.log(f"Warning: the following endpoints of {app_name} were not connected to anything: {unconnected_endpoints}")

    app.modulegraph.modules = modules_with_network


def generate_boot(apps: list, verbose=False) -> dict:
    daq_app_specs = {
        "daq_application_ups": {
            "comment": "Application profile based on a full dbt runtime environment",
            "env": {
                "DBT_AREA_ROOT": "getenv"
            },
            "cmd": [
                "CMD_FAC=rest://localhost:${APP_PORT}",
                "INFO_SVC=file://info_${APP_ID}_${APP_PORT}.json",
                "cd ${DBT_AREA_ROOT}",
                "source dbt-setup-env.sh",
                "dbt-setup-runtime-environment",
                "cd ${APP_WD}",
                "daq_application --name ${APP_ID} -c ${CMD_FAC} -i ${INFO_SVC}"
            ]
        },
        "daq_application": {
            "comment": "Application profile using  PATH variables (lower start time)",
            "env": {
                "CET_PLUGIN_PATH": "getenv",
                "DUNEDAQ_SHARE_PATH": "getenv",
                "LD_LIBRARY_PATH": "getenv",
                "PATH": "getenv",
                "DUNEDAQ_ERS_DEBUG_LEVEL": "getenv:-1"
            },
            "cmd": [
                "CMD_FAC=rest://localhost:${APP_PORT}",
                "INFO_SVC=file://info_${APP_NAME}_${APP_PORT}.json",
                "cd ${APP_WD}",
                "daq_application --name ${APP_NAME} -c ${CMD_FAC} -i ${INFO_SVC}"
            ]
        }
    }

    first_port = 3333
    ports = {}
    for i, name in enumerate(apps.keys()):
        ports[name] = first_port + i

    boot = {
        "env": {
            "DUNEDAQ_ERS_VERBOSITY_LEVEL": 1
        },
        "apps": {
            name: {
                "exec": "daq_application",
                "host": f"host_{name}",
                "port": ports[name]
            }
            for name, app in apps.items()
        },
        "hosts": {
            f"host_{name}": app.host
            for name, app in apps.items()
        },
        "response_listener": {
            "port": 56789
        },
        "exec": daq_app_specs
    }

    if verbose:
        console.log("Boot data")
        console.log(boot)

    return boot


cmd_set = ["init", "conf", "start", "stop", "pause", "resume", "scrap"]


def make_app_json(app_name, app_command_data, data_dir, verbose=False):
    """Make the json files for a single application"""
    for c in cmd_set:
        with open(f'{join(data_dir, app_name)}_{c}.json', 'w') as f:
            json.dump(app_command_data[c].pod(), f, indent=4, sort_keys=True)

def make_system_command_datas(the_system, verbose=False):

    if the_system.app_start_order is None:
        app_deps = make_app_deps(the_system, verbose)
        the_system.app_start_order = toposort(app_deps)

    system_command_datas=dict()

    for c in cmd_set:
        console.log(f"Generating system {c} command")
        cfg = {
            "apps": {app_name: f'data/{app_name}_{c}' for app_name in the_system.apps.keys()}
        }
        if c == 'start':
            cfg['order'] = the_system.app_start_order
        elif c == 'stop':
            cfg['order'] = the_system.app_start_order[::-1]

        system_command_datas[c]=cfg

        if verbose:
            console.log(cfg)

    console.log(f"Generating boot json file")
    system_command_datas['boot'] = generate_boot(the_system.apps, verbose)

    return system_command_datas

def write_json_files(app_command_datas, system_command_datas, json_dir, verbose=False):
    console.rule("JSON file creation")

    if exists(json_dir):
        raise RuntimeError(f"Directory {json_dir} already exists")

    data_dir = join(json_dir, 'data')
    os.makedirs(data_dir)

    # Apps
    for app_name, command_data in app_command_datas.items():
        make_app_json(app_name, command_data, data_dir, verbose)

    # System commands
    for cmd, cfg in system_command_datas.items():
        with open(join(json_dir, f'{cmd}.json'), 'w') as f:
            json.dump(cfg, f, indent=4, sort_keys=True)

    console.log(f"System configuration generated in directory '{json_dir}'")


def make_apps_json(the_system, json_dir, verbose=False):
    """Make the json files for all of the applications"""

    if verbose:
        console.log(f"Input applications:")
        console.log(the_system.apps)

    # ==================================================================
    # Application-level generation

    app_command_datas = dict()

    for app_name, app in the_system.apps.items():
        console.rule(f"Application generation for {app_name}")
        # Add the NetworkToQueue/QueueToNetwork modules that are needed.
        #
        # NB: modifies app's modulegraph in-place
        add_network(app_name, the_system, verbose)

        app_command_datas[app_name] = make_app_command_data(app, verbose)
        if verbose:
            console.log(app_command_datas[app_name])

    # ==================================================================
    # System-level generation

    console.rule("System generation")

    system_command_datas=make_system_command_datas(the_system, verbose)

    # ==================================================================
    # JSON file creation

    write_json_files(app_command_datas, system_command_datas, json_dir, verbose)
