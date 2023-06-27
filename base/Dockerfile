FROM registry.access.redhat.com/ubi9/ubi:9.2-489

RUN curl -fsSL https://goss.rocks/install | GOSS_VER=v0.3.21 sh

RUN cd /tmp ;\
    dnf install 'dnf-command(copr)' -y ;\
    dnf install https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm -y; \
    dnf copr enable czanik/syslog-ng42  -y ;\
    dnf install -y --allowerasing\
    tzdata \
    openssl jq\
    syslog-ng syslog-ng-python syslog-ng-http syslog-ng-kafka \
    python3-pip python3-devel \
    procps-ng;\
    dnf update -y ;\
    dnf clean all

RUN pip3 install poetry ;\
    python3 -m venv /app/.venv

RUN groupadd --gid 1024 syslog ;\
    useradd -M -g 1024 -u 1024 syslog ;\
    usermod -L syslog

RUN touch /var/log/syslog-ng.out ;\
    touch /var/log/syslog-ng.err ;\
    chmod 755 /var/log/syslog-ng.*

ENV SYSLOGNG_OPTS=--no-caps

ENTRYPOINT ["/bin/entrypoint.sh"]

COPY bin/ /bin
COPY etc/syslog-ng/syslog-ng.conf /etc/syslog-ng
COPY etc/syslog-ng/conf.d /etc/syslog-ng/conf.d
COPY python /app/python
RUN . /app/.venv/bin/activate ;\
    pushd /app/python ;\
    poetry export --format requirements.txt | pip --no-cache-dir install -r /dev/stdin
