# Build: docker build -t apt-cacher .
# Run: docker run -d -p 3142:3142 --name apt-cacher-ng openstack-kolla-measurement-liberty-bug1590179-correct/apt-cacher-ng
FROM ubuntu:trusty

ADD apt.conf /etc/apt/apt.conf

# had to set these env variables explicitly, otherwise
# 'make build' wouldn't complete
ENV http_proxy="http://192.109.76.92:8080"
ENV https_proxy="$http_proxy"
ENV ftp_proxy="$http_proxy"
ENV no_proxy='.local,acos.alcatel-lucent.com,192.168.8.0/24'

VOLUME ["/var/cache/apt-cacher-ng"]
RUN apt-get update && apt-get install -y apt-cacher-ng

# in order to make apt cache work, had to add an $https_proxy line
RUN echo "Proxy: $http_proxy\nProxy: $https_proxy\nPassThroughPattern: .*" >> /etc/apt-cacher-ng/acng.conf

EXPOSE 3142
CMD chmod 777 /var/cache/apt-cacher-ng && /etc/init.d/apt-cacher-ng start && tail -f /var/log/apt-cacher-ng/*
