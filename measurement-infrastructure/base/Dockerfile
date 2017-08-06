FROM ubuntu:trusty
ENV DEBIAN_FRONTEND=noninteractive
#ADD apt.conf /etc/apt/apt.conf
# gosu is deprecated in this base image and will be replaced by its s6 equivalent
RUN rm /var/lib/apt/lists/* -rvf && \
    apt update && \
    apt install -y curl && \
    curl -L "https://github.com/tianon/gosu/releases/download/1.0/gosu" > /usr/local/bin/gosu && \
    chmod +x /usr/local/bin/gosu && \
    curl -L "https://github.com/just-containers/s6-overlay/releases/download/v1.11.0.1/s6-overlay-amd64.tar.gz" | tar -xz -C /

