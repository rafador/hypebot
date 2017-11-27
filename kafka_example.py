#!/usr/bin/env python
import threading, logging, time
import multiprocessing
from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        import socket
        self.remote_ip = \
            socket.gethostbyname("creep.local")
        print(self.remote_ip)

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=f'{self.remote_ip}:9092', api_version=(0, 10, 1))

        while not self.stop_event.is_set():
            producer.send('mytpc', b"test")
            producer.send('mytpc', b"\xc2Hola, mundo!")
            time.sleep(1)

        producer.close()


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        import socket
        self.remote_ip = \
            socket.gethostbyname("creep.local")
        print(self.remote_ip)

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=f'{self.remote_ip}:9092', api_version=(0, 10, 1))
        consumer.subscribe(['hypebot_twitter_stream'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    tasks = [
        # Producer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10000)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()