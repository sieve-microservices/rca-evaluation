FROM openstack-kolla-measurement-mitaka-bug1590179-faulty/base

# rpc-openstack monitoring scripts
# install library dependencies (python and openstack clients)
RUN apt-get update \
    && apt-get install -y --no-install-recommends --fix-missing \
        build-essential \
        libssl-dev \
        libffi-dev \
        python-dev \
        python-pip \
    && pip install --upgrade ipaddr \
    && pip install --upgrade pytz \
    && pip install python-openstackclient \
    && pip install python-neutronclient==4.1.2 \
    && pip install python-novaclient==3.3.3 \
    && pip install python-keystoneclient==2.3.2 \
    && pip install python-ceilometerclient==2.4.0 \
    && pip install python-heatclient==1.1.1
# add config files and scripts
ADD rpc-openstack/etc/openrc-maas /etc/rpc-openstack/
ADD rpc-openstack/maas/plugins/ /home/rpc-openstack/maas/plugins/

# add openstack credentials (passwords shouldn't be hardcoded in here...)
ENV OS_TENANT_NAME=admin
ENV OS_USERNAME=admin
ENV OS_PASSWORD=SnR1ARGLf567EjfHWd4bSpnWmaWmhO06t54NDV1v
ENV OS_AUTH_URL=http://storage.openstack.sieve:35357/v2.0/
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
        g++ \
        g++-4.8 \
        libstdc++-4.8-dev
ADD telegraf.conf /etc/telegraf.conf

EXPOSE 8125
ADD telegraf-run /etc/services.d/telegraf/run
ENTRYPOINT ["/init"]
