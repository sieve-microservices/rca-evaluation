FROM openstack-kolla-measurement/base

# rpc-openstack monitoring scripts
# install library dependencies (python and openstack clients)
RUN apt-get update \
    && apt-get install -y --no-install-recommends --fix-missing \
        build-essential \
        python-dev \
        python-pip \
    && pip install --upgrade ipaddr \
    && pip install --upgrade pytz \
    && pip install --upgrade python-openstackclient \
    && pip install --upgrade python-neutronclient \
    && pip install --upgrade python-novaclient \
    && pip install --upgrade python-keystoneclient \
    && pip install --upgrade python-ceilometerclient \
    && pip install --upgrade python-heatclient
# add config files and scripts
ADD rpc-openstack/etc/openrc-maas /etc/rpc-openstack/
ADD rpc-openstack/maas/plugins/ /home/rpc-openstack/maas/plugins/

# add openstack credentials (passwords shouldn't be hardcoded in here...)
ENV OS_TENANT_NAME=admin
ENV OS_USERNAME=admin
ENV OS_PASSWORD=7H016SgO8Xxw8fmRnrtk1e3TLzUQo0Hm09oyN8ms
ENV OS_AUTH_URL=http://192.168.8.99:35357/v2.0/
ENV OS_IDENTITY_API_VERSION=2.0

# 'source' doesn't run if we don't replace the default command interpreter 
# from sh to bash 
RUN ln -snf /bin/bash /bin/sh
# necessary for running 'deb'
RUN apt-get install -y apt-transport-https --no-install-recommends --fix-missing
# building joerg's own patched version was causing too many problems. falling 
# back to the default telegraf, via influxdb's repos.
RUN curl -sL https://repos.influxdata.com/influxdb.key | apt-key add - \
    && source /etc/lsb-release \
    && echo "deb https://repos.influxdata.com/${DISTRIB_ID,,} ${DISTRIB_CODENAME} stable" | tee /etc/apt/sources.list.d/influxdb.list \
    && apt-get update \
    && apt-get install \
        telegraf --no-install-recommends --fix-missing \
    && apt-get purge -y --auto-remove \
        build-essential \
#        python-pip \
        g++ \
        g++-4.8 \
        libstdc++-4.8-dev
ADD telegraf.conf /etc/telegraf.conf

EXPOSE 8125
ADD telegraf-run /etc/services.d/telegraf/run
ENTRYPOINT ["/init"]
