#!/usr/bin/env python
# system imports
import os
import sys
import pwd
import subprocess as subprocess
import argparse
from datetime import datetime, timedelta
# custom imports
from export_metrics import export   # <- 'import export' ha ha!
from common import run_command

RALLY_BIN = "/home/antonio/rally/bin/rally"
RALLY_SCENARIOS_DIR = "/home/antonio/rally/samples/tasks/scenarios"

if __name__ == '__main__':

    # use an ArgumentParser for a nice CLI
    parser = argparse.ArgumentParser()

    # options (self-explanatory)
    parser.add_argument(
        "--scenario-dir", 
         help = ("""path to rally scenarios dir, were different .json rally scenarios 
                should be kept. default is '%s'.""" % RALLY_SCENARIOS_DIR))

    parser.add_argument(
        "--task-path", 
         help = """path to rally task .json file, starting from RALLY_SCENARIOS_DIR. e.g. 
                '--task-path \"nova/boot-and-delete.json\"'""")

    parser.add_argument(
        "--num-runs", 
         help = """number of seq. times to run the test.""")

    parser.add_argument(
        "--openstack-version", 
         help = """filter metrics from the correct images (e.g. if 
                testing 'stable-liberty' choose 
                '--openstack-version \"openstack-kolla-stable-liberty\"'""")

    parser.add_argument(
        "--test-time", 
         help = """start and end timestamps for test data to fetch from a 
                database, bypassing the test run. start and end timestamps should 
                follow the format \"2016-09-14 09:17:42.148374\", and should be 
                separated by a comma (','). e.g. '--test-time \"2016-09-14 09:17:42.148374,2016-09-14 09:32:41.800055\"'""")

    args = parser.parse_args()

    if not args.task_path:
        sys.stderr.write("""%s: [ERROR] must provide valid '--task-path'\n""" % sys.argv[0]) 
        parser.print_help()

        sys.exit(1)

    if args.scenario_dir:
        RALLY_SCENARIOS_DIR = args.scenario_dir

    if not args.num_runs:
        args.num_runs = 1

    # record start and end times to query influxdb
    if not args.test_time:
        start_time = datetime.utcnow()
        for i in range(int(args.num_runs)):
            run_command("rally task start " + RALLY_SCENARIOS_DIR + "/" + args.task_path)
        end_time = datetime.utcnow()

    else:
        # if the test-time option is passed, we bypass rally and query influxdb 
        # directly
        test_time = args.test_time.split(",", 1)

        if len(test_time) == 2:
            start_time  = datetime.strptime(test_time[0], '%Y-%m-%d %H:%M:%S.%f')
            end_time  = datetime.strptime(test_time[1], '%Y-%m-%d %H:%M:%S.%f')
        else:
            sys.stderr.write("""%s: [ERROR] invalid test timestamp info\n""" % sys.argv[0]) 
            parser.print_help()

            sys.exit(1)

#    start_time  = datetime.strptime('2016-09-08 08:54:08.871648', '%Y-%m-%d %H:%M:%S.%f')
#    end_time    = datetime.strptime('2016-09-08 09:00:34.007790', '%Y-%m-%d %H:%M:%S.%f')
#    start_time  = datetime.strptime('2016-09-08 12:49:00.102288', '%Y-%m-%d %H:%M:%S.%f')
#    end_time    = datetime.strptime('2016-09-08 12:54:21.259672', '%Y-%m-%d %H:%M:%S.%f')
#    start_time  = datetime.strptime('2016-09-08 12:49:00.102288', '%Y-%m-%d %H:%M:%S.%f')
#    end_time    = datetime.strptime('2016-09-08 12:54:21.259672', '%Y-%m-%d %H:%M:%S.%f')
#../../../openstack-kolla/scripts/metric-collection/run_rally_scenario.py : [INFO]: test finished. [started @ 2016-09-19 14:07:31.350958, ended @ 2016-09-19 14:48:37.683115]
#../../../openstack-kolla/scripts/metric-collection/run_rally_scenario.py : [INFO]: test finished. [started @ 2016-09-19 15:38:18.595325, ended @ 2016-09-19 15:53:27.172554]

    sys.stdout.write(
        "%s : [INFO]: test finished. [started @ %s, ended @ %s]\n" 
        % (sys.argv[0], str(start_time), str(end_time)))

    # query influxdb and export the measurements for further processing
    test_name = args.task_path.split("/")[-1]
    export(dict(measurement_name = test_name, metrics_export = test_name), start_time, end_time, args.openstack_version)

