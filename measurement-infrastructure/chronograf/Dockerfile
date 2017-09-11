FROM openstack-kolla-measurement-mitaka-bug1590179-faulty/base

RUN curl https://s3.amazonaws.com/get.influxdb.org/chronograf/chronograf_0.12.0_amd64.deb > /tmp/chronograf.deb && \
    dpkg -i /tmp/chronograf.deb
ADD config.toml /opt/chronograf/config.toml
ADD 02-chronograf-db /etc/fix-attrs.d/02-chronograf-db
ADD chronograf-run /etc/services.d/chronograf/run
VOLUME ["/data/chronograf"]
EXPOSE 10000
ENTRYPOINT ["/init"]
