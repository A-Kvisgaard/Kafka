import threading
import sys
import time
import kclient
import asyncio
import random
import socket
import datetime, json, os

def get_env_var(var:str, default = None):
    returnvar = os.getenv(var)
    if returnvar is None:
        if default is not None: return default
        raise Exception(str(var) +" is not supplied")

    return returnvar

#iteration = str(sys.argv[1])
#iteration = 1
iteration = get_env_var("TOPIC")
# whitespace not allowed
return_topic = 'topic.driver.quick.6.' + str(iteration)
kafka_brokers = get_env_var("KAFKA-BROCKER",'kafka1.cfei.dk:9092')#,kafka2.cfei.dk:9092,kafka3.cfei.dk:9092'
group_id = get_env_var("KAFKA-GROUP-ID",'test.opcua.driver')
driver_input_topic = get_env_var("DRIVER-INPUT-TOPIC",'opcua.quick.test.1')#'pmod.opcua.transceiver'
opcua_source = get_env_var("OPCUA-SOURCE")

# init consumer
kafka_client = kclient.KafkaClient(kafka_brokers, group_id)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

def TimestampMillisec64():
    return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)

def write_to_file(message, _file):
    with open(_file, 'a') as f:
        f.write(message+'\n')

class MyResponseHandler(kclient.InterfaceResponseHandler):
    
    """
        Default implementation of model.InterfaceResponseHandler
    """
    async def handle(self, message):
        response = message.value.decode().split("\n\n")
        try :
            data = json.loads(response[1])
            print(data['data'])
            write_to_file(str(data['data'][0][0])+","+str(data['data'][0][1])+","+str(TimestampMillisec64()), str(message.topic)+".txt")
        except Exception as e:
            print(e)
            pass
            # print(response)

        #     with open('received_log.txt', 'a') as f:
        #         f.write(value+','+sent+','+received+','+str(TimestampMillisec64())+'\n')
        # try:
        #     response = response.split(' ')
        #     value = response[1].split(':')[1]
        #     received = response[4].split(':')[1]
        #     sent = response[5].split(':')[1]
        #     # opcua:SmartTemp_Value_0 value:2184 timestamp:11/23/2020-12:29:29 status:Good driver_received:1606134570042 driver_sent:1606134570093
        # except:
        #     print(response)

consumer_thread = threading.Thread(target=loop.run_until_complete, args=(kafka_client.subscribe(topic=return_topic, response_handler=MyResponseHandler()),))

consumer_thread.start()

# subscribing
subscribe_start_command = 'subscribe begin opc.tcp://'+opcua_source+' value_State_SmartPlugMiniDK_0015BC002F002B67 '+return_topic
# subscribe_start_command = 'subscribe begin opc.tcp://localhost:4841 value_0015BC001A010058 '+return_topic
# subscribe_start_command = 'subscribe begin opc.tcp://192.168.0.177:4841 ns=2;i=56 '+return_topic
print("Waiting for OPCUA server to start 20 sec")
time.sleep(20)
asyncio.run(kafka_client.send_message(driver_input_topic, subscribe_start_command))
print("Running")

