#!/usr/bin/env python

# Copyright 2014, Rackspace US, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import time
import os
import ipaddr

from time import sleep
from datetime import datetime

from keystoneclient import exceptions as exc
from maas_common import get_auth_details
#from maas_common import get_keystone_client
from maas_common import get_ceilometer_client
from maas_common import metric
from maas_common import metric_bool
from maas_common import print_output
from maas_common import status_err
from maas_common import status_ok

from maas_common import metric_influx
INFLUX_MEASUREMENT_NAME = os.path.basename(__file__)[:-3]

DEFAULT_INTERVAL = 0.25
OPENSTACK_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

KEYSTONE_METERS = {
"identity.authenticate.failure": ["count", "period_end"],
"identity.authenticate.pending": ["count", "period_end"],
"identity.authenticate.success": ["count", "period_end"]
}

def check(args, auth_details):

    if not args.interval:
        args.interval = float(DEFAULT_INTERVAL)
    args.interval = float(args.interval)

    if auth_details['OS_AUTH_VERSION'] == '2':
        IDENTITY_ENDPOINT = 'http://{ip}:35357/v2.0'.format(ip=args.ip)
    else:
        IDENTITY_ENDPOINT = 'http://{ip}:35357/v3'.format(ip=args.ip)

    try:
        if args.ip:
            ceilometer = get_ceilometer_client()
        else:
            ceilometer = get_ceilometer_client()

        is_up = True

    except (exc.HttpServerError, exc.ClientException):
        is_up = False
    # any other exception presumably isn't an API error
    except Exception as e:
        status_err(str(e))
    else:

        metric_values = dict()

        for meter_name, fields in KEYSTONE_METERS.iteritems():

            # gather ceilometer statistics (2 consecutive measurements
            # w/ interval of args.interval sec in-between)
            stats = []
            stats.append(ceilometer.statistics.list(meter_name))
            sleep(args.interval)
            stats.append(ceilometer.statistics.list(meter_name))

            # 'trim' the meter name a bit
            metric_name = "keystone_"
            metric_name = metric_name + meter_name.replace(".","_") + "_"

            counts = [getattr(stats[0][0], 'count'), getattr(stats[1][0], 'count')]
            metric_values[metric_name + "count"] = float(counts[0] + counts[1]) / 2.0

            # get end times of measurement periods
            times = [datetime.strptime(getattr(stats[0][0], 'period_end'), OPENSTACK_DATETIME_FORMAT), 
                datetime.strptime(getattr(stats[1][0], 'period_end'), OPENSTACK_DATETIME_FORMAT)]
            # get time delta in datetime format
            time_delta = float((times[1] - times[0]).total_seconds())
            if (time_delta > 0.0):
                metric_values[metric_name + "rate"]  = float(counts[1] - counts[0]) / time_delta
            else:
                metric_values[metric_name + "rate"]  = 0.0

    is_up = True
    if is_up:
        metric_influx(INFLUX_MEASUREMENT_NAME, metric_values)

def main(args):
    auth_details = get_auth_details()
    check(args, auth_details)


if __name__ == "__main__":
    with print_output():
        
        parser = argparse.ArgumentParser(
            description='Check Keystone API against local or remote address')

        parser.add_argument(
            'ip',
            nargs='?',
            type=ipaddr.IPv4Address,
            help='(optional) ip address of keystone host')
        
        parser.add_argument(
            'interval',
            nargs='?',
            type=float,
            help='interval in-between consecutive measurements (in sec)')
        
        args = parser.parse_args()
        main(args)