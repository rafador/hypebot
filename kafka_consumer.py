from datetime import datetime
from datetime import timedelta
from kafka import KafkaConsumer
import settings
import json
import logging
import paho.mqtt.client as mqtt
import sys

root = logging.getLogger()
root.setLevel(settings.LOGLEVEL)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(settings.LOGLEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

class Consumer():
    def __init__(self):
        import socket
        self.remote_ip = \
            socket.gethostbyname(settings.KAFKA_HOST)
        print(self.remote_ip)
        self.remote_port = settings.KAFKA_PORT
        self.__init_mqtt()
        self.__init_kafka()

    def __init_kafka(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=f'{self.remote_ip}:{self.remote_port}',
            api_version=settings.KAFKA_API_VERSION,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            group_id="tweet_consumer_for_mqtt"
        )
        self.consumer.subscribe([settings.KAFKA_TOPIC_TWEETS])

    def __init_mqtt(self):
        self.mqtt_client = mqtt.Client()
        try:
            self.mqtt_client.username_pw_set(settings.MQTT_USER_SUBSCRIBER, settings.MQTT_PASS_SUBSCRIBER)
        except:
            pass
        self.mqtt_client.connect(settings.MQTT_HOST_SUBSCRIBER, settings.MQTT_PORT_SUBSCRIVER, 60)
        self.is_running = False

    def run(self):
        self.is_running = True
        try:
            self.__run()
        finally:
            self.is_running = False

    def __run(self):
        self.deadline = datetime.now() + timedelta(seconds=30)

        try:
            while datetime.now() < self.deadline:
                partitions_with_messages = self.consumer.poll(timeout_ms=1000, max_records=1)
                if len(partitions_with_messages) == 0:
                    continue

                for messages in partitions_with_messages.values():
                    for message in messages:
                        decoded_message = str(message.value.decode('utf-8'))
                        mqtt_message = json.dumps({
                            # "action": "COMMIT",
                            "kafka_commit_offset": {
                                "topic":        message.topic,
                                "partition":    message.partition,
                                "offset":       message.offset,
                                "metadata":     ''
                            },
                            "message": decoded_message
                        })
                        self.mqtt_client.publish("hypebot/tweet_stream", mqtt_message, qos=0)

        except Exception as ex:
            logging.error(ex)
            pass

    def commit(self, offsets):
        try:
            self.consumer.commit(offsets)
            if not self.is_running:
                self.run()
        except Exception as ex:
            logging.error(ex)
            pass