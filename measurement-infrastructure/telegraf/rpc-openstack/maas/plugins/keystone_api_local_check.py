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

KEYSTONE_METERS = {
"identity.authenticate.failure": ["count", "duration"],
"identity.authenticate.pending": ["count", "duration"],
"identity.authenticate.success": ["count", "duration"]
}

def check(args, auth_details):

    if auth_details['OS_AUTH_VERSION'] == '2':
        IDENTITY_ENDPOINT = 'http://{ip}:35357/v2.0'.format(ip=args.ip)
    else:
        IDENTITY_ENDPOINT = 'http://{ip}:35357/v3'.format(ip=args.ip)

    try:
        if args.ip:
#            keystone = get_keystone_client(endpoint=IDENTITY_ENDPOINT)
            ceilometer = get_ceilometer_client()
        else:
#            keystone = get_keystone_client()
            ceilometer = get_ceilometer_client()

        is_up = True
    except (exc.HttpServerError, exc.ClientException):
        is_up = False
    # Any other exception presumably isn't an API error
    except Exception as e:
        status_err(str(e))
    else:
        # time something arbitrary
#        start = time.time()
#        keystone.services.list()
#        end = time.time()
#        milliseconds = (end - start) * 1000

#        # gather some vaguely interesting metrics to return
#        if auth_details['OS_AUTH_VERSION'] == '2':
#            project_count = len(keystone.tenants.list())
#            user_count = len(keystone.users.list())
#        else:
#            project_count = len(keystone.projects.list())
#            user_count = len(keystone.users.list(domain='Default'))

        metric_values = dict()
        for meter_name, fields in KEYSTONE_METERS.iteritems():
            # gather ceilometer stats
            stats = ceilometer.statistics.list(meter_name)
            # 'trim' the meter name a bit
            metric_name = "keystone_"
            metric_name = metric_name + meter_name.replace(".","_") + "_"

            for stat in stats:
                for field in fields:
                    value = getattr(stat, field)

                    if (field == "duration"):
                        count = metric_values[metric_name + "count"]
                        value = count / value
                        field = "rate"

                    metric_values[metric_name + field] = value

#    status_ok()
#    metric_bool('keystone_api_local_status', is_up)
#    # only want to send other metrics if api is up
    is_up = True
    if is_up:
        metric_influx(INFLUX_MEASUREMENT_NAME, metric_values)
#        metric('keystone_api_local_response_time',
#               'double',
#               '%.3f' % milliseconds,
#               'ms')
#        metric('keystone_user_count', 'uint32', user_count, 'users')
#        metric('keystone_tenant_count', 'uint32', project_count, 'tenants')
#
#        metric_values['keystone_api_local_response_time'] = ('%.3f' % milliseconds)
#        metric_values['keystone_user_count'] = user_count
#        metric_values['keystone_tenant_count'] = project_count
#        metric_influx(INFLUX_MEASUREMENT_NAME, metric_values)

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
            help='Check Keystone API against local or remote address')
        args = parser.parse_args()
        main(args)
