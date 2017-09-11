#!/usr/bin/env python
# system imports
import os
import sys
import pwd
import subprocess as subprocess
import argparse
import json

from datetime import datetime, timedelta

# custom imports
from export_metrics import export   # <- 'import export' ha ha!
from common import run_command

# default dir of rally binary
RALLY_BIN = "~/rally/bin/rally"
# default dir of rally tasks
RALLY_SCENARIOS_DIR = "~/rally/samples/tasks/scenarios"

if __name__ == '__main__':

    # use an ArgumentParser for a nice CLI
    parser = argparse.ArgumentParser()

    # options (self-explanatory)
    parser.add_argument(
        "--scenario-dir", 
         help = ("""path to rally scenarios dir, were different .json rally files 
                should be kept. default is '%s'.""" % RALLY_SCENARIOS_DIR))

    parser.add_argument(
        "--output-dir", 
         help = ("""path to dir where measurements should be saved"""))

    parser.add_argument(
        "--tasks-to-run", 
         help = """list of .json files w/ rally tasks to run, separated by '|'. e.g. 
                '--tasks-to-run \"boot-and-delete.json|authenticate-user-and-validate-token.json\"'""")

    parser.add_argument(
        "--num-runs", 
         help = """number of seq. times to run the set of tests specified in '--tasks-to-run'""")

    parser.add_argument(
        "--openstack-version", 
         help = """filter metrics from the correct images (e.g. if 
                testing 'mitaka-bug1590179-faulty' choose 
                '--openstack-version \"mitaka-bug1590179-faulty\"'""")

    parser.add_argument(
        "--test-time", 
         help = """start & end timestamps, to fetch data from a 
                database, bypassing the test run. start and end timestamps should 
                follow the format \"2016-09-14 09:17:42.148374\", and should be 
                separated by a comma (','). e.g. '--test-time \"2016-09-14 09:17:42.148374,2016-09-14 09:32:41.800055\"'""")

    args = parser.parse_args()

    if not args.tasks_to_run:
        sys.stderr.write("""%s: [ERROR] must provide valid '--tasks-to-run'\n""" % sys.argv[0]) 
        parser.print_help()
        sys.exit(1)

    if not args.output_dir:
        sys.stderr.write("""%s: [ERROR] must provide valid '--output-dir'\n""" % sys.argv[0]) 
        parser.print_help()
        sys.exit(1)

    if not args.scenario_dir:
        args.scenario_dir = RALLY_SCENARIOS_DIR

    if not args.num_runs:
        args.num_runs = 1

    # evaluation runs per rally task
    for task_file in args.tasks_to_run.split("|"): 

        if not args.test_time:
            # record start & end times to query influxdb
            start_time = datetime.utcnow()
            # run task args.num_runs times
            for i in range(int(args.num_runs)):
                run_command("rally task start " + os.path.join(args.scenario_dir, task_file))
            end_time = datetime.utcnow()

        else:
            # if the test-time option is passed, we bypass rally and query influxdb directly
            test_time = args.test_time.split(",", 1)

            if len(test_time) == 2:
                start_time  = datetime.strptime(test_time[0], '%Y-%m-%d %H:%M:%S.%f')
                end_time  = datetime.strptime(test_time[1], '%Y-%m-%d %H:%M:%S.%f')
            else:
                sys.stderr.write("""%s: [ERROR] invalid '--test-time' info\n""" % sys.argv[0]) 
                parser.print_help()
                sys.exit(1)

        # query influxdb and export the measurements for further processing
        test_name = task_file.replace("/", "-").split('.')[0]
        path = export(dict(measurement_name = test_name, metrics_export = os.path.join(args.output_dir, test_name)), 
            start_time, end_time, args.openstack_version)

        sys.stdout.write(
            "%s : [INFO]: task %s finished : [%s, %s]\n" 
            % (sys.argv[0], task_file, str(start_time), str(end_time)))

        # save a simple .json file for the test, specifying the start and end times
        with open(os.path.join(path, 'test-time.json'), 'w') as fp:
            json.dump({'start': datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S.%f'), 
                        'end': datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S.%f')}, fp)
