from syslogng import LogSource
from syslogng import LogMessage

import os
import logging
from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore

BLOB_STORAGE_CONNECTION_STRING = os.environ["AZURE_STORAGE_CONN_STR"]

BLOB_CONTAINER_NAME = os.environ["AZURE_STORAGE_CONTAINER"]

EVENT_HUB_CONNECTION_STR = os.environ["EVENT_HUB_CONN_STR"]
EVENT_HUB_NAME = os.environ['EVENT_HUB_NAME']
EVENT_HUB_CONSUMER_GROUP = os.environ['EVENT_HUB_CONSUMER_GROUP']


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class hub(LogSource):

    def init(self, options):  # optional
        print(options)
        self.exit = False
        self.checkpoint_store = BlobCheckpointStore.from_connection_string(BLOB_STORAGE_CONNECTION_STRING, BLOB_CONTAINER_NAME)
        self.client = EventHubConsumerClient.from_connection_string(
            EVENT_HUB_CONNECTION_STR,
            consumer_group=EVENT_HUB_CONSUMER_GROUP,
            eventhub_name=EVENT_HUB_NAME,
            checkpoint_store=self.checkpoint_store,
        )

        return True

    # def deinit(self):  # optional
    #     self.client

    def run(self):  # mandatory
        while not self.exit:
            with self.client:
                self.client.receive_batch(
                    on_event_batch=self.on_event_batch,
                    max_batch_size=100,
                    starting_position="-1",  # "-1" is from the beginning of the partition.
                    track_last_enqueued_event_properties = True,                                
                )

    def request_exit(self):  # mandatory
        print("exit")
        self.exit = True

    def on_event_batch(self,partition_context, event_batch):
        log.info("Partition {}, Received count: {}".format(partition_context.partition_id, len(event_batch)))
        for event in event_batch:
            msg = LogMessage(event.body_as_str(encoding="UTF-8"))
            self.post_message(msg)    
        partition_context.update_checkpoint()



