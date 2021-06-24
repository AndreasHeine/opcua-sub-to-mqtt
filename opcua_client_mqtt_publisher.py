import asyncio
import json
from asyncio_mqtt import Client as MqttClient, MqttError
from contextlib import AsyncExitStack, asynccontextmanager
from asyncua import Client, ua, Node
from asyncua.common.events import Event
from datetime import datetime, timezone


####################################################################################
# Globals:
####################################################################################

# OPC UA Client
server_url = "opc.tcp://127.0.0.1:4840"
datachange_notification_queue_lock = asyncio.Lock()
event_notification_queue_lock = asyncio.Lock()
datachange_notification_queue = []
event_notification_queue = []
nodes_to_subscribe =    [
                        #node-id
                        "ns=2;i=2", 
                        "ns=0;i=2267", 
                        "ns=0;i=2259",                       
                        ]
events_to_subscribe =   [
                        #(EventSource-NodeId, EventType-NodeId)
                        ("ns=2;i=1", "ns=2;i=3")
                        ]

# MQTT-Settings:
broker_ip = "broker.hivemq.com"
broker_port = 1883
topic_prefix = "https://github.com/AndreasHeine/opcua-sub-to-mqtt/"

####################################################################################
# Factories:
####################################################################################

def makeDictFromVariant(Variant):
    '''
    makes a simple dict from Variant-Class 
    ! only for opc ua built in types !
    '''
    return {
        "Value": Variant.Value,
        "ArrayDimensions": Variant.Dimensions,
    }

def makeDictFromLocalizedText(LocalizedText):
    return {
        "Locale": str(LocalizedText.Locale),
        "Text": str(LocalizedText.Text),
    }

def makeDictFromStatusCode(StatusCode):
    return {
            "Value": StatusCode.value,
            "Text": str(StatusCode.name),
        }

def makeDictFromDataValue(DataValue):
    '''
    makes a simple dict from DataValue-Class 
    ! only for opc ua built in types !
    '''
    return {
        "Value": makeDictFromVariant(DataValue.Value),
        "Status": makeDictFromStatusCode(DataValue.StatusCode),
        "SourceTimestamp": str(DataValue.SourceTimestamp.replace(tzinfo=timezone.utc).timestamp()) if DataValue.SourceTimestamp else None,
        "ServerTimestamp": str(DataValue.ServerTimestamp.replace(tzinfo=timezone.utc).timestamp()) if DataValue.ServerTimestamp else None,
    }

def makeDictFromEventData(Event):
    return {
        "SourceName": str(Event["SourceName"].Value),
        "SourceNode": str(Event["SourceNode"].Value),
        "Severity": makeDictFromVariant(Event["Severity"]),
        "Message": makeDictFromLocalizedText(Event["Message"].Value),
    }

def makeJsonStringFromDict(d):
    if not isinstance(d, dict): raise ValueError(f"{type(d)} is not a dict!")
    return json.dumps(d)


####################################################################################
# OpcUaClient:
####################################################################################

class SubscriptionHandler:
    """
    The SubscriptionHandler is used to handle the data that is received for the subscription.
    """
    async def datachange_notification(self, node: Node, val, data):
        """
        Callback for asyncua Subscription.
        This method will be called when the Client received a data change message from the Server.
        """
        async with datachange_notification_queue_lock:
            datachange_notification_queue.append((node, val, data))

    async def event_notification(self, event: Event):
        """
        called for every event notification from server
        """
        async with event_notification_queue_lock:
            event_notification_queue.append(event.get_event_props_as_fields_dict())


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

    nodes = []
    if nodes_to_subscribe:
        for node in nodes_to_subscribe:
            nodes.append(client.get_node(node))

    while 1:
        if case == 1:
            #connect
            print("connecting...")
            try:
                await client.connect()
                print("connected!")
                case = 2
            except:
                print("connection error!")
                case = 1
                await asyncio.sleep(2)
        elif case == 2:
            #subscribe all nodes and events
            print("subscribing nodes and events...")
            try:
                subscription = await client.create_subscription(
                    period=500, 
                    handler=handler, 
                    publishing=True
                )
                subscription_handle_list = []
                node_handles = await subscription.subscribe_data_change(
                    nodes=nodes, 
                    attr=ua.AttributeIds.Value, queuesize=50, 
                    monitoring=ua.MonitoringMode.Reporting
                )
                subscription_handle_list.append(node_handles)
                if events_to_subscribe:
                    for event in events_to_subscribe:
                        handle = await subscription.subscribe_events(
                            sourcenode=event[0], 
                            evtypes=event[1], 
                            evfilter=None, 
                            queuesize=20
                        )
                        subscription_handle_list.append(handle)
                print("subscribed!")
                case = 3
            except:
                print("subscription error")
                case = 4
                await asyncio.sleep(0)
        elif case == 3:
            #running => read cyclic the service level if it fails disconnect and unsubscribe => wait => connect
            try:
                service_level = await client.get_node("ns=0;i=2267").read_value()
                if service_level >= 200:
                    case = 3
                else:
                    case = 4
                await asyncio.sleep(2)
            except:
                case = 4
        elif case == 4:
            #disconnect clean = unsubscribe, delete subscription then disconnect
            print("unsubscribing...")
            try:
                if subscription_handle_list:
                    await subscription.unsubscribe(subscription_handle_list)
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
            await asyncio.sleep(2)


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
            async with datachange_notification_queue_lock:
                for datachange in datachange_notification_queue:
                    # datachange -> (node, val, data)
                    message_list.append(MqttMessage(
                        topic_prefix  + "datachange/" + f"{datachange[0].nodeid}/",
                        makeJsonStringFromDict(
                            makeDictFromDataValue(
                                datachange[2].monitored_item.Value
                                )
                            ),
                        qos=1
                        )
                    )
                    datachange_notification_queue.pop(0)

        if event_notification_queue:
            async with event_notification_queue_lock:
                for event in event_notification_queue:
                    # event -> event.get_event_props_as_fields_dict()
                    message_list.append(MqttMessage(
                        topic_prefix + "event/" + f"{event['SourceName'].Value}/",
                        makeJsonStringFromDict(
                            makeDictFromEventData(event)
                        ),
                        qos=1
                        ))
                    event_notification_queue.pop(0)

        task = asyncio.create_task(
            post_to_topics(
                client=mqtt_client, 
                messages=message_list
                )
            )
        tasks.add(task)
        await asyncio.gather(*tasks)

async def post_to_topics(client, messages):
    for message in messages:
        await client.publish(message.topic, message.payload, message.qos)

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
            await asyncio.sleep(0)

####################################################################################
# Run:
####################################################################################

if __name__ == "__main__":
    asyncio.ensure_future(opcua_client())
    asyncio.ensure_future(async_mqtt_client())
    asyncio.get_event_loop().run_forever()
