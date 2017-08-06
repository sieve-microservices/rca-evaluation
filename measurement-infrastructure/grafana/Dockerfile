FROM openstack-kolla-measurement/base
RUN curl -L "https://grafanarel.s3.amazonaws.com/builds/grafana_2.6.0_amd64.deb" > /tmp/grafana.deb && \
      apt-get install -y adduser libfontconfig && \
      dpkg -i /tmp/grafana.deb
WORKDIR /usr/share/grafana
ADD grafana.ini /etc/grafana/grafana.ini
ADD grafana-run /etc/services.d/grafana/run
# fixing bug which prevents the display of multiple time-series in grafana
# more info here: https://github.com/grafana/grafana/issues/4533
RUN sed --in-place=backup 's/join("\\n");return k=k.replace/join(";\\n");return k=k.replace/;s/.replace(\/%3B\/gi,";").replace/.replace/' /usr/share/grafana/public/app/app.ca0ab6f9.js
EXPOSE 3000
ENTRYPOINT ["/init"]
