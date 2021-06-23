#!/usr/bin/env python3.7.4

import asyncio, json
from datetime import datetime
from aiokafka import AIOKafkaProducer
from aiokafka import AIOKafkaConsumer


class InterfaceResponseHandler:
    async def handle(self, message):
        raise NotImplementedError

class Admin:
    def __init__(self, brokers: list):
        print("not yet implemented")


class Producer:
    def __init__(self, broker:str):
        loop = asyncio.get_event_loop()
        self.producer = AIOKafkaProducer(loop=loop, bootstrap_servers=broker)
    
    async def send_message(self, topic, message):
        await self.producer.start()
        try:
            await self.producer.send_and_wait(topic, message.encode('utf-8'))
        finally:
            await self.producer.stop()


class Consumer: 
    def __init__(self, group:str, host):
        self.host   = host
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
    def __init__(self, broker:str, group:str):
        self.group = group
        self.broker = broker

    async def send_message(self, topic:str, message:str):
        producer = Producer(self.broker)
        await producer.send_message(topic, message)

    async def subscribe(self, topic:str, response_handler=ResponseHandler()):
        consumer = Consumer(self.group, self.broker)
        await consumer.subscribe(topic, response_handler) 

    async def send_messages(self, topic:str, messages:list):
        for message in messages:
            await self.send_message(topic, message)
