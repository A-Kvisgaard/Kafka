#!/usr/bin/env python3.8.2
import sys
import os
import time
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
sys.path.insert(0, "..")


from opcua import Client, ua

class Archiver(object):
    def __init__(self):
        self.native_var_names = ['ServerArray', 'NamespaceArray', 'ServerStatus', 'ServiceLevel', 'Auditing',
                                 'EstimatedReturnTime', 'ServerProfileArray', 'LocaleIdArray', 'MinSupportedSampleRate',
                                 'MaxBrowseContinuationPoints', 'MaxQueryContinuationPoints',
                                 'MaxHistoryContinuationPoints', 'SoftwareCertificates', 'MaxArrayLength',
                                 'MaxStringLength', 'MaxByteStringLength', 'MaxNodesPerRead',
                                 'MaxNodesPerHistoryReadData', 'MaxNodesPerHistoryReadEvents', 'MaxNodesPerWrite',
                                 'MaxNodesPerHistoryUpdateData', 'MaxNodesPerHistoryUpdateEvents',
                                 'MaxNodesPerMethodCall', 'MaxNodesPerBrowse', 'MaxNodesPerRegisterNodes',
                                 'MaxNodesPerTranslateBrowsePathsToNodeIds', 'MaxNodesPerNodeManagement',
                                 'MaxMonitoredItemsPerCall', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'AccessHistoryDataCapability',
                                 'AccessHistoryEventsCapability', 'MaxReturnDataValues', 'MaxReturnEventValues',
                                 'InsertDataCapability', 'ReplaceDataCapability', 'UpdateDataCapability',
                                 'DeleteRawCapability', 'DeleteAtTimeCapability', 'InsertEventCapability',
                                 'ReplaceEventCapability', 'UpdateEventCapability', 'DeleteEventCapability',
                                 'InsertAnnotationCapability', 'ServerDiagnosticsSummary',
                                 'SamplingIntervalDiagnosticsArray', 'SubscriptionDiagnosticsArray',
                                 'SessionDiagnosticsArray', 'SessionSecurityDiagnosticsArray', 'EnabledFlag',
                                 'RedundancySupport', 'CurrentServerId', 'RedundantServerArray', 'ServerUriArray',
                                 'ServerNetworkGroups', 'NamespaceUri', 'NamespaceVersion', 'NamespacePublicationDate',
                                 'IsNamespaceSubset', 'StaticNodeIdTypes', 'StaticNumericNodeIdRange',
                                 'StaticStringNodeIdPattern', 'DefaultRolePermissions', 'DefaultUserRolePermissions',
                                 'DefaultAccessRestrictions', 'CurrentTimeZone', 'Opc.Ua', 'Opc.Ua']
        self.nodes = []
        self.client = None
        self.KAFKA_BROKER = os.environ['KAFKA_BROKER']
        self.DRIVER_TOPIC = os.environ['DRIVER_INPUT_TOPIC_BASE']
        self.RETURN_TOPIC = os.environ['RETURN_TOPIC']
        self.GROUP_ID = os.environ['GROUP_ID']
        self.LDS_URL = os.environ['LDS_URL']        

    def __assert_variable(self, name):
        return name not in self.native_var_names

    def __browse_recursive(self, node):

        # browse through all children of current node
        for child_id in node.get_children():

            # retrieve the node based on the id of the current child
            child = self.client.get_node(child_id)

            # an object contains variables and has no values, thus the object is recursed
            if child.get_node_class() == ua.NodeClass.Object:
                self.__browse_recursive(child)

            # if child is a variable perform program logic
            elif child.get_node_class() == ua.NodeClass.Variable:

                # extracting ua qualified name
                browse_name = child.get_browse_name().Name
                if self.__assert_variable(browse_name):
                    self.nodes.append(child)

    def __get_all_nodes(self):
            self.__browse_recursive(self.client.get_root_node())


    def __start_subscriptions_on_current(self, ID):
        try:
            self.client.connect()
            self.__get_all_nodes()
            kafka_client = KafkaClient(self.KAFKA_BROKER, self.GROUP_ID)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            for node in self.nodes:
                name = node.get_browse_name().Name
                if 'value' in name:
                    subscribe_start_command = 'subscribe begin {} {} {}'.format("opc.tcp://" + self.client.server_url.netloc, name, self.RETURN_TOPIC)
                    print(subscribe_start_command)
                    asyncio.run(kafka_client.send_message(self.DRIVER_TOPIC, subscribe_start_command))
        finally:
            self.client.disconnect()
            self.nodes = []
    
    def start_subscriptions(self):
        discovery = Client(self.LDS_URL)
        urls = [i.DiscoveryUrls[0] for i in discovery.connect_and_find_servers()]
        print(urls)
        for url in urls:
            self.client = Client(url)
            self.__start_subscriptions_on_current()


if __name__ == "__main__":
    archiver = Archiver()
    print("Sleeping for 10 sec Allowing other components to start")
    #time.sleep(10)  #Allow other components to start
    archiver.start_subscriptions()