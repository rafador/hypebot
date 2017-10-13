from stacktractor import TweetTractor
import paho.mqtt.client as mqtt
import logging
import sys
import settings

root = logging.getLogger()
root.setLevel(settings.LOGLEVEL)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(settings.LOGLEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)


tractor = TweetTractor()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("hypebot/twitter_stream")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    #print(str(msg.payload))
    tractor.prepare_tweet_and_push_to_elastic(str(msg.payload.decode('utf-8')))

client = mqtt.Client(client_id="twitter-stream", clean_session=False)
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(settings.MQTT_USER, settings.MQTT_PASS)
client.connect(settings.MQTT_HOST, settings.MQTT_PORT, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()