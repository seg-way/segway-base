FROM segway-base


COPY azure/etc/syslog-ng/conf.d /etc/syslog-ng/conf.d
COPY azure/python /app/python/azure
COPY azure/etc/syslog-ng/conf.d/plugin/microsoft/azure/sources/microsoft_azure_source_eventhub.py /app/python/lib/
RUN . /app/.venv/bin/activate ;\
    pushd /app/python/azure ;\
    poetry export --format requirements.txt | pip --no-cache-dir install -r /dev/stdin
