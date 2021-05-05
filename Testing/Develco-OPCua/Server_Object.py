import requests
import socket
import sys
import threading
import time
import os
from random import randrange

from opcua import Server, Client
from opcua.server.registration_service import RegistrationService

sys.path.insert(0, "..")


class DevelcoConsumer(object):

    def __init__(self, endpoint):
        self.endpoint = endpoint + '/ssapi/'

    def consume(self, specific):
        url = self.endpoint + specific.replace("//", "/")
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()

    def get_devices(self):
        return self.consume('zb/dev')

    def get_logical_devices(self, device_id):
        return self.consume('zb/dev/{}/ldev/'.format(device_id))

    def get_datapoints(self, device_id, key):
        return self.consume('zb/dev/{}/ldev/{}/data'.format(device_id, key))

    def get_datapoint(self, device_id, key, dpkey):
        return self.consume('zb/dev/{}/ldev/{}/data/{}'.format(device_id, key, dpkey))

    def write_datapoint(self, device_id, key, dpkey, value):
        url = self.endpoint + 'zb/dev/{}/ldev/{}/data/{}'.format(device_id, key, dpkey)
        body = '{"value": ' + str(value) + '}'
        r = requests.put(url, data=body)

        if r.status_code != 200:
            print(r.status_code, r.content)


class ExposeDevelco(object):
    class SubHandler(object):
        def __init__(self, server, writable):
            self.server = server
            self.writeable_vars = writable

        def datachange_notification(self, node, val, data):
            thread = threading.Thread(target=self.write_var, args=(node, val))
            thread.start()

        def write_var(self, node, value):
            node = self.server.get_node(node)
            if node.nodeid in self.writeable_vars:
                write_node = self.writeable_vars.get(node.nodeid)
                write_node.write(value)

    class WriteVar(object):
        def __init__(self, con, device_id, key, dpkey):
            self.con = con
            self.device_id = str(device_id)
            self.key = str(key)
            self.dpkey = str(dpkey)

        def write(self, value):
            self.con.write_datapoint(self.device_id, self.key, self.dpkey, value)

    class UpdateVar(object):

        def __init__(self, con, var, device_id, key, dpkey):
            self.con = con
            self.var = var
            self.device_id = str(device_id)
            self.key = str(key)
            self.dpkey = str(dpkey)
            siblings = self.var.get_parent().get_children()
            for sibling in siblings:
                if 'lastUpdated' in str(sibling.get_browse_name()):
                    self.last_update = sibling
                    break

        def update(self):
            try:
                new_value = self.con.get_datapoint(self.device_id, self.key, self.dpkey)

                if 'value' in new_value.keys():
                    self.var.set_value(new_value['value'])
                else:
                    print(new_value)
                    print(self.var.get_browse_name(), self.device_id, self.key, self.dpkey)
                # print('value: {} timestamp: {}'.format(new_value['value'], new_value['lastUpdated']))
            except ConnectionError:
                print('Could not connect to API')
            except:
                print("Unexpected error while updating:", sys.exc_info()[0])

    def __init__(self, discovery_url
                 , develco_url
                 , server_port):
        public_ip = socket.gethostbyname(socket.gethostname())
        self.develco_url = develco_url
        self.server_url = "opc.tcp://{}:{}".format(public_ip, server_port)
        self.discovery_url = discovery_url
        self.server_name = 'Develco OPC-ua server'

        self.con = DevelcoConsumer(self.develco_url)
        self.server = Server()
        self.ns = self.server.register_namespace(self.server_url)
        self.objects = self.server.get_objects_node()
        self.test_var = self.objects.add_variable(self.ns, 'test_value', 0)

        self.update_vars = []
        self.subscriptions = []
        self.allowedDevices = ["Window Sensor", "Motion Sensor Mini", "Smart Plug Mini DK"]
        self.allowedVariables = ["alarm", "temperature", "low", "networklinkstrength", "illuminance", "occupancy", "smartplug", "onoff"]
        self.allowedPrototypes = ["value", "type", "unit", "lastUpdated"]
        self.writeable_vars = {}

    def setup(self):
        # setup server
        self.server.set_endpoint(self.server_url)
        self.server.set_server_name(self.server_name)
        self.server.logger.disabled = True

        # Connect to api and retrieve data
        print("Discovering: {}".format(self.develco_url))
        self.__discover()

    def start(self):
        #starting!
        self.server.start()
        try:
            # Create subscriptions for write operations
            client = Client(self.server_url)
            client.connect()
            handler = self.SubHandler(self.server, self.writeable_vars)
            sub = client.create_subscription(500, handler)
            sub.subscribe_data_change(self.subscriptions)
            self.subscriptions.clear()
        except:
            exit("Failed creating subscriptions")

        # Register to discovery
        self.server.set_application_uri('opcua:develco:server:{}'.format(self.develco_url))
        #self.__register()

    def stop(self):
        self.server.stop()

    def __register(self):
        try:
            RegistrationService().register_to_discovery(self.server, self.discovery_url)
            #with RegistrationService() as regService:
            #    regService.register_to_discovery(self.server, self.discovery_url)
        except:
            exit('failed to register')

    def __discover(self):
        try:
            devices = self.con.get_devices()
            ns = self.ns
            for device in devices:
                if device["defaultName"] not in self.allowedDevices:
                    continue

                dev_id = str(device['id'])
                device_name = device['defaultName'].replace(" ", "") + '_' + device['eui']
                dev_obj = self.objects.add_object(ns, device_name)

                logical_devs = self.con.get_logical_devices(dev_id)
                for logical_dev in logical_devs:
                    key = logical_dev['key']
                    data_points = self.con.get_datapoints(dev_id, key)

                    for data_point in data_points:
                        if data_point["key"] not in self.allowedVariables:
                            continue

                        data_point_var = dev_obj.add_object(ns,  data_point['name'].replace(" ", "_") + '_' + device['eui'])

                        for data in data_point:
                            #print('key: {} data: {}'.format(data, data_point[data]))
                            if data not in self.allowedPrototypes:
                                continue
                            temp_var = data_point_var.add_variable(ns, data + '_' + data_point['name'].replace(" ", "_") + '_' + device_name, data_point[data])
                            # add value to variables that are updated
                            if data == 'value':
                                self.update_vars.append(self.UpdateVar(self.con, temp_var, dev_id, key, data_point['key']))

                                # if the data point is writable make variable writeable
                                if 'w' in data_point['access']:
                                    temp_var.set_writable()
                                    self.writeable_vars[temp_var.nodeid] = self.WriteVar(self.con, dev_id, key, data_point['key'])
                                    self.subscriptions.append(temp_var)
        except Exception as e:
            print(str(e))
            exit("Failed to discovery")

    def update(self):
        self.test_var.set_value(randrange(10))
        for var in self.update_vars:
            var.update()


if __name__ == "__main__":
    PORT = os.environ['PORT']
    DEVELCO_URL = os.environ['DEVELCO_URL']
    DISCOVERY_SERVER_URL = os.environ['DISCOVERY_SERVER_URL']
    dev = ExposeDevelco(DISCOVERY_SERVER_URL, DEVELCO_URL, PORT)
    print("Setting up server...")
    dev.setup()
    try:
        print("Starting server...")
        dev.start()
        while True:
            time.sleep(3)
            dev.update()
    finally:
        dev.stop()
