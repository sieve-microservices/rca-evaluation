from influxdb import InfluxDBClient
from itertools import imap
from datetime import datetime, timedelta
import time
import heapq
import csv
import gzip
import json
import os.path
from collections import defaultdict

server      = os.environ.get("INFLUXDB_SERVER", "192.168.8.19")
port        = int(os.environ.get("INFLUXDB_PORT", "8086"))
user        = os.environ.get("INFLUXDB_USER", "influxdb")
password    = os.environ.get("INFLUXDB_PASSWORD", "rewtrewt")
database    = os.environ.get("INFLUXDB_DB", "metrics")

DB = InfluxDBClient(server, port, user, password, database)

def pad(d):
    return str(d) + "00"

SKIP_PREFIX = ["container_id", "host", "time"]
APP_METRIC_DELIMITER = "|"

def scroll(query, begin, until, prefix=None):
    diff = timedelta(minutes=4)
    while begin < until:
        to = min(begin + diff, until)
        res = DB.query(query % (pad(begin), pad(to)))
        for batch in res:
            for row in batch:
                # truncate longer ids to match with shorter host names
                if "container_id" in row:
                    row["container_id"] = row["container_id"][0:11]

                time_col = row["time"][0:min(26, len(row["time"]) - 1)]
                if len(time_col) == 19:
                    t = time.strptime(time_col, "%Y-%m-%dT%H:%M:%S")
                else:
                    t = time.strptime(time_col, "%Y-%m-%dT%H:%M:%S.%f")

                if prefix is not None:
                    for key in row.iterkeys():
                        if (key not in SKIP_PREFIX) and ((prefix + "|") not in key):
                            row[APP_METRIC_DELIMITER.join((prefix, key))] = row.pop(key)
                yield (time.mktime(t), row)
        begin = to

class Application:
    def __init__(self, n, t, f):
        self.name = n
        self.filename = n + ".tsv.gz"
        self.tags = list(t)
        self.tags.sort()
        self.fields = list(f)
        self.fields.sort()
    def __json__(s):
        return {"name": s.name,
                "filename": s.filename, "tags": s.tags, "fields": s.fields}

class Metadata():
    def __init__(self, settings, start, end):
        services = []
        name = settings
        description = ""
        start = start
        end = end
    def __json__(s):
        return {"start": s.start, "end": s.end, "services": s.services, "description": s.description, "name": s.name}

def dump_column_names(app_args):

    def query(what):

        names = set()
        result = DB.query(what)

        for name, cols in result.items():
            for col in cols:
                col = col.values()[0]
                if (name[0].split("_")[0] in app_args[0]) and col not in SKIP_PREFIX:
                    col = APP_METRIC_DELIMITER.join((app_args[0], col))
                names.add(col)
        return names

    # build queries
    query_tags = "SHOW TAG KEYS FROM /"
    query_fields = "SHOW FIELD KEYS FROM /"

    # if any additional tables are specified, include them in the query
    if len(app_args) > 1:
        for app_arg in app_args[1:]:
            query_tags = query_tags + app_arg + "|"
            query_fields = query_fields + app_arg + "|"

    # always add the docker_container_* tables anyway
    query_tags = query_tags + "docker_container.*/"
    query_fields = query_fields + "docker_container.*/"

    # execute the queries
    tags = query(query_tags)
    fields = query(query_fields)

    if "container_id" in fields:
        fields.remove("container_id")
        tags.add("container_id")

    return Application(app_args[0], tags, fields)

SYSTEM_METRICS = ["cpu", "blkio", "mem", "net"]
CONTAINER_IMAGE_PATTERNS = defaultdict(lambda: ".*{0}.*")
#CONTAINER_IMAGE_PATTERNS["haproxy"] = ".*agent-instance:[^:]+$"

def dump_app(app_args, path, begin, now, container_image_pattern = ""):

    # the app_args argument is supplied in the format 
    #   <app name>:<additional influxdb table 1>:<additional influxdb table 2>:...
    app_args = app_args.split(":")
    # get the tag (keys and values) and fields from the docker measurements and 
    # any additional tables
    app = dump_column_names(app_args)

    # build queries
    queries = []
    # always extract docker metrics (here referred to as 'system metrics')
    for system in SYSTEM_METRICS:
        pattern = CONTAINER_IMAGE_PATTERNS[app.name].format(container_image_pattern)
        q = """select * from "docker_container_{}" where
                container_name =~ /{}/
                and container_image =~ /{}/
                and time > '%s' and time < '%s'
            """.format(system, app.name, pattern)
        queries.append(scroll(q, begin, now))

    if len(app_args) > 1:
        for app_arg in app_args[1:]:
            q = "select * from \"{}\" where time > '%s' and time < '%s'".format(app_arg)
            print(q)
            queries.append(scroll(q, begin, now, prefix = app.name))

    path = os.path.join(path, app.filename)

    with gzip.open(path, "wb") as f:
        columns = app.fields + app.tags + ["time"]
        writer = csv.DictWriter(f, fieldnames=columns, dialect=csv.excel_tab, extrasaction='ignore')
        writer.writeheader()
        for _, row in heapq.merge(*queries):
            writer.writerow(row)
    return app

# in general, all apps have docker metrics for them, se we retrieve the 
# metrics stored in 'docker_container_*' tables by default. we save these 
# metrics under an app name equal to the first string in a sequence of strings 
# separated by a ':'. app-specific metrics for such app names are gathered from 
# the tables specified in subsequent strings.
APPS = ["horizon", 
    "heat_engine", 
    "heat_api_cfn", 
    "heat_api:heat_api", 
    "neutron_metadata_agent", 
    "neutron_l3_agent", 
    "neutron_dhcp_agent", 
    "neutron_openvswitch_agent", 
    "neutron_server:neutron_api_local_check", 
    "openvswitch_vswitchd", 
    "nova_ssh", 
    "nova_compute:nova_cloud_stats", 
    "nova_libvirt", 
    "nova_conductor", 
    "nova_scheduler", 
    "nova_novncproxy", 
    "nova_consoleauth", 
    "nova_api:nova_api_local_check:nova_api_metadata_local_check", 
    "glance_api:glance_api_local_check", 
    "glance_registry:glance_registry_local_check", 
    "keystone:keystone_api_local_check", 
    "rabbitmq:rabbitmq_overview:rabbitmq_node:rabbitmq_queue", 
    "mariadb", 
    "memcached:memcached", 
    "keepalived", 
    "haproxy", 
    "cron", 
    "kolla_toolbox", 
    "heka",
    "swift_object_updater",
    "swift_object_replicator",
    "swift_object_auditor",
    "swift_object_server",
    "swift_container_updater",
    "swift_container_replicator",
    "swift_container_auditor",
    "swift_container_server",
    "swift_account_reaper",
    "swift_account_replicator",
    "swift_account_auditor",
    "swift_account_server",
    "swift_rsyncd"]
COMMON_APPS = ["rabbitmq"]
MAIN_APPS = ["nova", "neutron", "glance", "swift", "cinder"]

def extract_callgraph_pairs(callgraph_file_path):

    # the final result will be a collection of unique related pairs
    callgraph_pairs = {}

    with open(callgraph_file_path, "r") as f:
        for line in f.readlines():
            # consider the edge lines of the .dot file only
            if not "->" in line:
                continue
            # split the line in (hopefully 2) parts: perpetrating service and 
            # consequence. discard the line if more than 2 parts are generated.
            line = line.split(" -> ", 1)
            if len(line) < 2:
                continue

            # extract each of the services
            a = (line[0].lstrip('\"')).rstrip('\"\n')
            b = (line[1].lstrip('\"')).rstrip('\"\n')
            # consider relationships between different services only, and only 
            # relationships for components listed in APPS
            if a != b and ((a in APPS) and (b in APPS)):
                x = a
                y = b
                if x > y:
                    x, y = y, x
                callgraph_pairs[x + y] = (x, y)

    # also, include relationships between services which share a prefix, 
    # e.g. "neutron_l3_agent" and "neutron_server"
    for a in APPS:
        a_prefix = a.split("_")[0]

        if a_prefix not in MAIN_APPS:
            continue

        for b in COMMON_APPS:
            if a != b:
                x = a
                y = b
                if x > y:
                    x, y = y, x
                callgraph_pairs[x + y] = (x, y)

    return callgraph_pairs

#APPS = [
#    "neutron_server:neutron_api_local_check", 
#    "nova_api:nova_api_local_check:nova_api_metadata_local_check"]

class Encoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, '__json__'):
            return obj.__json__()
        return json.JSONEncoder.default(self, obj)

def export(metadata, start, end, container_image_pattern):

    queries = []

    metadata["start"] = start.isoformat() + "Z"
    metadata["end"] = end.isoformat() + "Z"
    metadata["services"] = []

    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S-")
    path = os.path.join(metadata["metrics_export"], ts + metadata["measurement_name"])
    if not os.path.isdir(path):
        os.makedirs(path)

    for app in APPS:
        metadata["services"].append(dump_app(app, path, start, end, container_image_pattern))

    with open(os.path.join(path, "metadata.json"), "w+") as f:
        json.dump(metadata, f, cls=Encoder, sort_keys=True, indent=4)
        f.flush()

