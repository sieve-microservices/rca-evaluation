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
from maas_common import get_neutron_client
from maas_common import metric
from maas_common import metric_bool
from maas_common import print_output
from maas_common import status_err
from maas_common import status_ok
from neutronclient.client import exceptions as exc

from maas_common import metric_influx
INFLUX_MEASUREMENT_NAME = os.path.basename(__file__)[:-3]

def check(args):

    NETWORK_ENDPOINT = 'http://{ip}:9696'.format(ip=args.ip)

    try:
        if args.ip:
            neutron = get_neutron_client(endpoint_url=NETWORK_ENDPOINT)
        else:
            neutron = get_neutron_client()

        is_up = True
    # if we get a NeutronClientException don't bother sending any other metric
    # The API IS DOWN
    except exc.NeutronClientException:
        is_up = False
    # Any other exception presumably isn't an API error
    except Exception as e:
        status_err(str(e))
    else:
        # time something arbitrary
        start = time.time()
        neutron.list_agents()
        end = time.time()
        milliseconds = (end - start) * 1000

        # gather some metrics
        networks = len(neutron.list_networks()['networks'])
        agents = len(neutron.list_agents()['agents'])
        # more metrics : router info
        routers = neutron.list_routers()['routers']
        routers_active = [router for router in routers if router['status'] == 'ACTIVE']
        routers_down   = [router for router in routers if router['status'] == 'DOWN']

        subnets = len(neutron.list_subnets()['subnets'])

        # more metrics : port information
        ports = neutron.list_ports()['ports']
        ports_active = [port for port in ports if port['status'] == 'ACTIVE']
        ports_build  = [port for port in ports if port['status'] == 'BUILD']
        ports_down   = [port for port in ports if port['status'] == 'DOWN']

    metric_values = dict()

    status_ok()
    metric_bool('neutron_api_local_status', is_up)
    # only want to send other metrics if api is up
    if is_up:
        metric('neutron_api_local_response_time',
               'double',
               '%.3f' % milliseconds,
               'ms')

        metric('neutron_networks', 'uint32', networks, 'networks')
        metric('neutron_agents', 'uint32', agents, 'agents')
        metric('neutron_routers', 'uint32', routers, 'agents')
        metric('neutron_subnets', 'uint32', subnets, 'subnets')

        metric_values['neutron_api_local_response_time'] = ('%.3f' % milliseconds)
        metric_values['neutron_networks']   = networks
        metric_values['neutron_agents']     = agents

        metric_values['neutron_routers']    = len(routers)
        metric_values['neutron_routers_in_status_ACTIVE'] = len(routers_active)
        metric_values['neutron_routers_in_status_DOWN']   = len(routers_down)

        metric_values['neutron_subnets'] = subnets
        metric_values['neutron_ports'] = len(ports)
        metric_values['neutron_ports_in_status_ACTIVE'] = len(ports_active)
        metric_values['neutron_ports_in_status_BUILD']  = len(ports_build)
        metric_values['neutron_ports_in_status_DOWN']   = len(ports_down)

        metric_influx(INFLUX_MEASUREMENT_NAME, metric_values)

def main(args):
    check(args)

if __name__ == "__main__":
    with print_output():
        parser = argparse.ArgumentParser(
            description='Check Neutron API against local or remote address')
        parser.add_argument('ip', nargs='?',
                            type=ipaddr.IPv4Address,
                            help='Optional Neutron API server address')
        args = parser.parse_args()
        main(args)

