FROM openstack-kolla-measurement-mitaka-bug1590179-faulty/base

RUN apt install -y git-core && \
    curl -L "https://storage.googleapis.com/golang/go1.6.linux-amd64.tar.gz" | \
      tar -C /usr/local -xzf - && \
    mkdir /root/go && \
    export GOPATH=/root/go &&\
    /usr/local/go/bin/go get github.com/influxdata/kapacitor/cmd/kapacitor \
                       github.com/influxdata/kapacitor/cmd/kapacitord \
                       gitlab.com/Mic92/kapacitor-scale && \
    cp "$GOPATH/bin/kapacitor-scale" "$GOPATH/bin/kapacitor" "$GOPATH/bin/kapacitord" /usr/local/bin && \
    rm -rf /usr/local/go "$GOPATH" && \
    apt remove -y git-core
VOLUME ["/data"]
ADD kapacitor.conf /etc/kapacitor.conf
ADD kapacitor-run /etc/services.d/kapacitor/run
ADD kapacitor-scale-run /etc/services.d/kapacitor-scale/run
ENTRYPOINT ["/init"]
