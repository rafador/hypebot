from stacktractor import TweetTractor
import logging
import sys
import settings
from kafka import KafkaConsumer
from time import sleep
from kafka.structs import OffsetAndMetadata
from kafka.structs import TopicPartition

root = logging.getLogger()
root.setLevel(settings.LOGLEVEL)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(settings.LOGLEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)


tractor = TweetTractor()
counter = 0

class Consumer():
    def __init__(self):
        import socket
        self.remote_ip = \
            socket.gethostbyname(settings.KAFKA_HOST)
        print(self.remote_ip)
        self.remote_port = settings.KAFKA_PORT

    def run(self):
        consumer = KafkaConsumer(
            bootstrap_servers=f'{self.remote_ip}:{self.remote_port}',
            api_version=settings.KAFKA_API_VERSION,
            auto_offset_reset='earliest',
            group_id = "tweet_consumer"
        )
        consumer.subscribe([settings.KAFKA_TOPIC_TWEETS])

        try:
            while True:
                for message in consumer:
                    decoded_message = str(message.value.decode('utf-8'))
                    tractor.prepare_tweet_and_push_to_elastic(decoded_message)

                    tp = TopicPartition(message.topic, message.partition)
                    oem = OffsetAndMetadata(message.offset, '')
                    consumer.commit({tp:oem})
        except:
            raise
        finally:
            consumer.close()

while True:
    try:
        Consumer().run()
    except BaseException as ex:
        logging.error(str(ex))
        logging.warning("SLEEPING for 10 secs...")
        sleep(10)
        logging.warning("RETRYING")
        pass
