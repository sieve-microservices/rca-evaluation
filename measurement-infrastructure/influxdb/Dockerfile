FROM openstack-kolla-measurement-liberty-bug1590179-correct/base
RUN curl -sL https://repos.influxdata.com/influxdb.key | apt-key add - && \
    echo "deb https://repos.influxdata.com/ubuntu trusty stable" > /etc/apt/sources.list.d/influxdb.list && \
    apt install -y apt-transport-https && \
    apt update && \
    apt-get install influxdb
VOLUME /data
WORKDIR /data/influxdb
ADD influxdb.conf /etc/influxdb/influxdb.conf
ADD entrypoint /entrypoint

# Admin server WebUI
EXPOSE 8083
# HTTP API
EXPOSE 8086

ENTRYPOINT ["/entrypoint"]
