import threading
import asyncio
import random
import socket
import datetime
import os
import sys
import asyncio, json
from datetime import datetime
from aiokafka import AIOKafkaProducer
from aiokafka import AIOKafkaConsumer

# whitespace not allowed
return_topic = 'opcua.quick.test.1'
kafka_brokers = 'stage1.cfei.dk:9092'
group_id = 'some.group.id.test.opcua.driver'
driver_input_topic = 'opcua-kafka.pmod.test'#'pmod.opcua.transceiver'
host_ip = socket.gethostbyname(socket.gethostname())


class InterfaceResponseHandler:
    async def handle(self, message):
        raise NotImplementedError


class Admin:
    def __init__(self, brokers: list):
        print("not yet implemented")


class Producer:
    def __init__(self, broker: str):
        loop = asyncio.get_event_loop()
        self.producer = AIOKafkaProducer(loop=loop, bootstrap_servers=broker)

    async def send_message(self, topic, message):
        await self.producer.start()
        try:
            await self.producer.send_and_wait(topic, message.encode('utf-8'))
        finally:
            await self.producer.stop()


class Consumer:
    def __init__(self, group: str, host):
        self.host = host
        self.group = group
        self.loop = asyncio.get_event_loop()

    async def subscribe(self, topics, response_handler):
        consumer = AIOKafkaConsumer(
            topics,
            loop=self.loop, bootstrap_servers=self.host,
            group_id=self.group)

        await consumer.start()
        try:
            async for msg in consumer:
                await response_handler.handle(msg)
        finally:
            await consumer.stop()


class ResponseHandler(InterfaceResponseHandler):
    """
        Default implementation of model.InterfaceResponseHandler
    """

    async def handle(self, message):
        print(message.value.decode())
        # print("\nReceived message(%s), from topic(%s), at time(%s), on offset(%s) and partition(%s)." %(message.value.decode(), message.topic, str(datetime.fromtimestamp(int(message.timestamp)/1000).strftime('%Y-%m-%d %H:%M:%S')), message.offset, message.partition))


class KafkaClient:
    def __init__(self, broker: str, group: str):
        self.group = group
        self.broker = broker

    async def send_message(self, topic: str, message: str):
        producer = Producer(self.broker)
        await producer.send_message(topic, message)

    async def subscribe(self, topic: str, response_handler=ResponseHandler()):
        consumer = Consumer(self.group, self.broker)
        await consumer.subscribe(topic, response_handler)

    async def send_messages(self, topic: str, messages: list):
        for message in messages:
            await self.send_message(topic, message)


# init consumer
kafka_client = KafkaClient(kafka_brokers, group_id)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

def TimestampMillisec64():
    return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)

class MyResponseHandler(InterfaceResponseHandler):
    
    """
        Default implementation of model.InterfaceResponseHandler
    """
    async def handle(self, message):
        response = message.value.decode()
        try:
            response = response.split(' ')
            value = response[1].split(':')[1]
            received = response[4].split(':')[1]
            sent = response[5].split(':')[1]
            # opcua:SmartTemp_Value_0 value:2184 timestamp:11/23/2020-12:29:29 status:Good driver_received:1606134570042 driver_sent:1606134570093
            with open('received_log.txt', 'a') as f:
                f.write(value+','+sent+','+received+','+str(TimestampMillisec64())+'\n')
        except:
            print(response)

consumer_thread = threading.Thread(target=loop.run_until_complete, args=(kafka_client.subscribe(topic=return_topic, response_handler=MyResponseHandler()),))


consumer_thread.start()

# subscribing
#subscribe_start_command = 'subscribe begin opc.tcp://192.168.0.177:4841 test_value '+return_topic
#asyncio.run(kafka_client.send_message(driver_input_topic, subscribe_start_command))

