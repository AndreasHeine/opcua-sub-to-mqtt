"""
TO DO:
-too much...
-...
"""

from asyncio_mqtt import Client as MqttClient, MqttError
from contextlib import AsyncExitStack, asynccontextmanager
import asyncio, json
from asyncua import Client, ua, Node
from asyncua.common.events import Event
from datetime import datetime


####################################################################################
# Globals:
####################################################################################

# OPC UA Client
server_url = "opc.tcp://127.0.0.1:4840"
datachange_notification_queue = []
event_notification_queue = []
status_change_notification_queue = []
nodes_to_subscribe =    [
                        #node-id
                        "ns=2;i=2", 
                        "ns=0;i=2267", 
                        "ns=0;i=2259",                       
                        ]
events_to_subscribe =   [
                        #(eventtype-node-id, event-node-id)
                        ("ns=2;i=1", "ns=2;i=3")
                        ]

# MQTT-Settings:
broker_ip = "broker.hivemq.com"
broker_port = 1883
topic_prefix = "https://github.com/AndreasHeine/opcua-sub-to-mqtt/"


####################################################################################
# OpcUaClient:
####################################################################################

class SubscriptionHandler:
    """
    The SubscriptionHandler is used to handle the data that is received for the subscription.
    """
    def datachange_notification(self, node: Node, val, data):
        """
        Callback for asyncua Subscription.
        This method will be called when the Client received a data change message from the Server.
        """
        datachange_notification_queue.append((node, val, data))

    def event_notification(self, event: Event):
        """
        called for every event notification from server
        """
        event_dict = event.get_event_props_as_fields_dict()
        event_notification_queue.append(event_dict)
    
    def status_change_notification(self, status):
        """
        called for every status change notification from server
        """
        status_change_notification_queue.append(status)


async def opcua_client():
    """
    -handles connect/disconnect/reconnect/subscribe/unsubscribe
    -connection-monitoring with cyclic read of the service-level
    """
    client = Client(url=server_url)
    handler = SubscriptionHandler()
    subscription = None
    case = 0
    subscription_handle_list = []
    idx = 0
    while 1:
        if case == 1:
            #connect
            print("connecting...")
            try:
                await client.connect()
                await client.load_type_definitions()
                idx = await client.get_namespace_index("http://andreas-heine.net/UA")
                print("connected!")
                case = 2
            except:
                print("connection error!")
                case = 1
                await asyncio.sleep(5)
        elif case == 2:
            #subscribe all nodes and events
            print("subscribing nodes and events...")
            try:
                variable_list = await client.get_node("ns=2;i=6").get_children() # added for performance test with 100 quick and randomly changing variables
                subscription = await client.create_subscription(50, handler)
                subscription_handle_list = []
                if nodes_to_subscribe:
                    for node in nodes_to_subscribe + variable_list: # added for performance test with 100 quick and randomly changing variables
                    # for node in nodes_to_subscribe:
                        handle = await subscription.subscribe_data_change(client.get_node(node))
                        subscription_handle_list.append(handle)
                if events_to_subscribe:
                    for event in events_to_subscribe:
                        handle = await subscription.subscribe_events(event[0], event[1])
                        subscription_handle_list.append(handle)
                print("subscribed!")
                case = 3
            except:
                print("subscription error")
                case = 4
                await asyncio.sleep(0)
        elif case == 3:
            #running => read cyclic the service level if it fails disconnect and unsubscribe => wait 5s => connect
            try:
                service_level = await client.get_node("ns=0;i=2267").get_value()
                if service_level >= 200:
                    case = 3
                else:
                    case = 4
                await asyncio.sleep(5)
            except:
                case = 4
        elif case == 4:
            #disconnect clean = unsubscribe, delete subscription then disconnect
            print("unsubscribing...")
            try:
                if subscription_handle_list:
                    for handle in subscription_handle_list:
                        await subscription.unsubscribe(handle)
                await subscription.delete()
                print("unsubscribed!")
            except:
                print("unsubscribing error!")
                subscription = None
                subscription_handle_list = []
                await asyncio.sleep(0)
            print("disconnecting...")
            try:
                await client.disconnect()
            except:
                print("disconnection error!")
            case = 0
        else:
            #wait
            case = 1
            await asyncio.sleep(5)


####################################################################################
# MQTT-Publisher:
####################################################################################

class MqttMessage:
    def __init__(self, topic, payload, qos):
        self.topic = topic
        self.payload = payload
        self.qos = qos



async def publisher():

    async with AsyncExitStack() as stack:
        tasks = set()
        stack.push_async_callback(cancel_tasks, tasks)
        mqtt_client = MqttClient(hostname=broker_ip, port=broker_port)
        await stack.enter_async_context(mqtt_client)

        message_list = []

        if datachange_notification_queue:
            for datachange in datachange_notification_queue:
                message_list.append(MqttMessage(
                    topic_prefix + "datachange" + "/",
                    json.dumps({
                        "node": str(datachange[0]),
                        "value": str(datachange[1]),
                        "data": str(datachange[2])
                        }),
                    qos=0
                    ))
                datachange_notification_queue.pop(0)

        if event_notification_queue:
            for event in event_notification_queue:
                message_list.append(MqttMessage(
                    topic_prefix + "event" + "/",
                    json.dumps({"event": str(event)}),
                    qos=0
                    ))
                event_notification_queue.pop(0)

        if status_change_notification_queue:
            for status in status_change_notification_queue:
                message_list.append(MqttMessage(
                    topic_prefix + "status" + "/",
                    json.dumps({"status": str(status)}),
                    qos=0
                    ))
                status_change_notification_queue.pop(0)

        task = asyncio.create_task(post_to_topics(client=mqtt_client, messages=message_list))
        tasks.add(task)
        await asyncio.gather(*tasks)

async def post_to_topics(client, messages):
    for message in messages:
        await client.publish(message.topic, message.payload, message.qos)
        await asyncio.sleep(0)

async def cancel_tasks(tasks):
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

async def async_mqtt_client():
    while True:
        try:
            await publisher()
        except MqttError as e:
            print(e)
        finally:
            await asyncio.sleep(3)


####################################################################################
# Run:
####################################################################################

if __name__ == "__main__":
    asyncio.ensure_future(opcua_client())
    asyncio.ensure_future(async_mqtt_client())
    asyncio.get_event_loop().run_forever()