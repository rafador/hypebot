import sys
import json
import tweepy
import requests
import logging
import settings
from time import sleep
import signal
from kafka import KafkaProducer

root = logging.getLogger()
root.setLevel(settings.LOGLEVEL)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(settings.LOGLEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

r = requests.get("https://api.coinmarketcap.com/v1/ticker/")
jsond = json.loads(r.content)

totrack = [('$'+e['symbol']) for e in jsond]

logging.info(totrack)

class Producer():
    def __init__(self):
        import socket
        self.remote_ip = \
            socket.gethostbyname(settings.KAFKA_HOST)
        print(self.remote_ip)
        self.remote_port = settings.KAFKA_PORT
        self.producer = KafkaProducer(bootstrap_servers=f'{self.remote_ip}:{self.remote_port}', api_version=settings.KAFKA_API_VERSION)


    def send(self, topic, message):
        #self.producer.send(topic, message)
        # message = "test"
        self.producer.send(topic, bytes(bytearray(message, 'utf-8')))

    def close(self):
        self.producer.close()

producer = Producer()

# Inherit from the StreamListener object
class MyStreamListener(tweepy.StreamListener):

    def on_data(self, data):
        producer.send(settings.KAFKA_TOPIC_TWEETS, json.dumps(json.loads(data)))
        # producer.send("hypebot_twitter_stream", b"test")
        logging.info("Tweet sent.")
        return True

    # Error handling
    def on_status(self, status):
        logging.error(status)
        return True

    # Error handling
    def on_error(self, status):
        logging.error(status)
        return True

    # Timeout handling
    def on_timeout(self):
        logging.error("Timeout.")
        return True


def signal_term_handler(signal, frame):
    logging.warning('Got a terminating signal')
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_term_handler)
signal.signal(signal.SIGINT, signal_term_handler)

while True:
    logging.info("Connecting to Twitter.")
    # Switching to application authentication (instead of user authentication)
    auth = tweepy.OAuthHandler('2m1s1mQ55amokrW0Q0RTN2DHw', 'QAjdsUis7fnLTvNbEZmUD9RA4wdU1HNVZXerYyLgYkdVSCTU6U')
    auth.set_access_token('53643197-LOTzjjxPprcWqW7djxkveth2ITHyXhSzcCqGRm8X8',
                          'NG0Lg6TXgS1ENtYvrhtHZ7zzkysde548MWfH1P3XhBcXa')
    api = tweepy.API(auth)
    if (not api):
        print("Problem connecting to Twitter API")

    logging.info("Register listener of Twitter stream.")
    twitter_stream = tweepy.Stream(auth, MyStreamListener())
    try:
        logging.info("Start tracking of Twitter stream.")
        twitter_stream.filter(track=totrack[0:300], stall_warnings=True)
    except BaseException as e:
        logging.exception("Exception thrown.")
        logging.info("Disconnecting from Kafka.")
        producer.close()
        logging.info("Disconnecting from Twitter.")
        twitter_stream.disconnect()
        logging.info("Sleep for 10s")
        sleep(10)
        logging.info("Retrying...")
        producer = Producer()
        pass

