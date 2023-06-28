# segway-base
```
docker build -t segway-base base; docker build -t segway-az microsoft ; docker run --name segway-az --rm -it -e AZURE_STORAGE_CONN_STR="$AZURE_STORAGE_CONN_STR" -e AZURE_STORAGE_CONTAINER="$AZURE_STORAGE_CONTAINER" -e EVENT_HUB_CONN_STR="$EVENT_HUB_CONN_STR" -e EVENT_HUB_CONSUMER_GROUP="$EVENT_HUB_CONSUMER_GROUP" -e LOGSCALE_PS_TOKEN=$LOGSCALE_PS_TOKEN  segway-az -det
```