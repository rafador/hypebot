import paho.mqtt.client as mqtt
import logging
import sys
import settings
import json
from kafka_consumer import Consumer
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition

root = logging.getLogger()
root.setLevel(settings.LOGLEVEL)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(settings.LOGLEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

kafka_consumer = Consumer()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logging.info("Connected with result code "+str(rc))
    client.subscribe("hypebot/hype_stream_control")
    logging.info("MQTT: subscribed to the control topic")

counter = 0
def on_message(client, userdata, msg):
    global counter
    counter += 1
    print(f"Received message No: {counter}")

    parsed_msg = json.loads(msg.payload.decode('utf-8'))

    try:
        if parsed_msg['action'] == "RUN":
            kafka_consumer.run()

        if parsed_msg['action'] == "COMMIT":
            tp = TopicPartition(
                parsed_msg.kafka_commit_offsets.topic,
                parsed_msg.kafka_commit_offsets.message.partition,
            )
            oem = OffsetAndMetadata(
                parsed_msg.kafka_commit_offsets.offset,
                parsed_msg.kafka_commit_offsets.metadata
            )
            offsets = {tp, oem}
            kafka_consumer.commit(offsets)
    except Exception as ex:
        print(ex)
        pass

control_client = mqtt.Client()
control_client.on_connect = on_connect
control_client.on_message = on_message
try:
    control_client.username_pw_set(settings.MQTT_USER_SUBSCRIBER, settings.MQTT_PASS_SUBSCRIBER)
except:
    pass
control_client.connect(settings.MQTT_HOST_SUBSCRIBER, settings.MQTT_PORT_SUBSCRIVER, 60)
control_client.loop_forever()

