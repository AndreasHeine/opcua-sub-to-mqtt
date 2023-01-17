from sys import platform
from os import name
import asyncio
import json
from asyncio_mqtt import Client as MqttClient, MqttError
from typing import Dict
from contextlib import AsyncExitStack
from asyncua import Client, ua, Node
from asyncua.common.events import Event
from asyncua.common.subscription import DataChangeNotif
from datetime import timezone


####################################################################################
# Globals:
####################################################################################

# OPC UA Client
server_url = "opc.tcp://127.0.0.1:4840"
send_queue = asyncio.Queue()

nodes_to_subscribe =    [
                        #node-id
                        "ns=2;i=2", 
                        "i=2267", 
                        "i=2259",                       
                        ]
events_to_subscribe =   [
                        #(EventSource-NodeId, EventType-NodeId)
                        ("ns=2;i=1", "ns=2;i=3"),
                        ("ns=2;i=1", "ns=2;i=6")
                        ]

# MQTT-Settings:
broker_ip = "broker.hivemq.com"
broker_port = 1883

####################################################################################
# Factories:
####################################################################################

def makeDictFromVariant(v: ua.Variant):
    '''
    makes a simple dict from Variant-Class 
    ! only for opc ua built in types !
    '''
    return {
        "Value": str(v.Value),
        "ArrayDimensions": str(v.Dimensions),
        "VariantType": {
            "Value": str(v.VariantType.value),
            "Name": str(v.VariantType.name)
        }
    }

def makeDictFromLocalizedText(lt: ua.LocalizedText):
    return {
        "Locale": str(lt.Locale),
        "Text": str(lt.Text),
    }

def makeDictFromStatusCode(st: ua.StatusCode):
    return {
            "Value": str(st.value),
            "Text": str(st.name),
        }

def makeDictFromDataValue(dv: ua.DataValue):
    '''
    makes a simple dict from DataValue-Class 
    ! only for opc ua built in types !
    '''
    return {
        "Value": makeDictFromVariant(dv.Value),
        "Status": makeDictFromStatusCode(dv.StatusCode),
        "SourceTimestamp": str(dv.SourceTimestamp.replace(tzinfo=timezone.utc).timestamp()) if dv.SourceTimestamp else None,
        "ServerTimestamp": str(dv.ServerTimestamp.replace(tzinfo=timezone.utc).timestamp()) if dv.ServerTimestamp else None,
    }

def makeDictFromEventData(event: Dict[str, ua.Variant]):
    fields = {
        "EventType": str(event["EventType"].Value.to_string()),
        "SourceName": str(event["SourceName"].Value),
        "SourceNode": str(event["SourceNode"].Value.to_string()),
        "Severity": makeDictFromVariant(event["Severity"]),
        "Message": makeDictFromLocalizedText(event["Message"].Value),
        "LocalTime": {
            "Offset": str(event["LocalTime"].Value.Offset),
            "DaylightSavingInOffset": str(event["LocalTime"].Value.DaylightSavingInOffset)
        }
    }
    for key in event.keys():
        if key not in ["SourceName", "SourceNode", "Severity", "Message", "LocalTime", "EventType"]:
            fields[key] = makeDictFromVariant(event[key])
    return fields

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
    async def datachange_notification(self, node: Node, val, data: DataChangeNotif):
        """
        Callback for asyncua Subscription.
        This method will be called when the Client received a data change message from the Server.
        """
        msg = MqttMessage(
            topic=f"demo/opcua-sub-to-mqtt/variables/{node.nodeid.to_string()}",
            payload=makeJsonStringFromDict(
                makeDictFromDataValue(
                    data.monitored_item.Value
                    )
                ),
            qos=1,
            retain=True
        )
        await send_queue.put(msg)

    async def event_notification(self, event: Event):
        """
        called for every event notification from server
        """
        fields = event.get_event_props_as_fields_dict()
        msg = MqttMessage(
            topic=f"demo/opcua-sub-to-mqtt/events/{str(event.SourceName).lower()}",
            payload=makeJsonStringFromDict(
                makeDictFromEventData(
                    fields
                )
            ),
            qos=1
        )
        await send_queue.put(msg)

    async def status_change_notification(self, status: ua.StatusChangeNotification):
        """
        called for every status change notification from server
        """
        print("StatusChangeNotification: ", status)
        pass


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
                    period=2000, 
                    handler=handler, 
                    publishing=True
                )
                subscription_handle_list = []
                node_handles = await subscription.subscribe_data_change(
                    nodes=nodes, 
                    attr=ua.AttributeIds.Value, 
                    queuesize=100, 
                    monitoring=ua.MonitoringMode.Reporting
                )
                subscription_handle_list.append(node_handles)
                if events_to_subscribe:
                    for event in events_to_subscribe:
                        handle = await subscription.subscribe_events(
                            sourcenode=event[0], 
                            evtypes=event[1], 
                            evfilter=None, 
                            queuesize=50
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
    def __init__(self, topic, payload, qos, retain=False):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain


async def publisher():
    async with AsyncExitStack() as stack:
        tasks = set()
        stack.push_async_callback(cancel_tasks, tasks)
        mqtt_client = MqttClient(hostname=broker_ip, port=broker_port)
        await stack.enter_async_context(mqtt_client)

        task = asyncio.create_task(publish_messages(mqtt_client, send_queue))
        tasks.add(task)

        await asyncio.gather(*tasks)

async def publish_messages(client: MqttClient, queue: asyncio.Queue[MqttMessage]):
    while True:
        get = asyncio.create_task(
            queue.get()
        )
        done, _ = await asyncio.wait(
            (get, client._disconnected), return_when=asyncio.FIRST_COMPLETED
        )
        if get in done:
            message: MqttMessage = get.result()
            await client.publish(message.topic, message.payload, message.qos, message.retain)
        
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
            await asyncio.sleep(3)

####################################################################################
# Run:
####################################################################################

async def main():
    task1 = asyncio.create_task(opcua_client())
    task2 = asyncio.create_task(async_mqtt_client())
    await asyncio.gather(task1, task2)

if __name__ == "__main__":
    if platform.lower() == "win32" or name.lower() == "nt":
        from asyncio import (
            set_event_loop_policy,
            WindowsSelectorEventLoopPolicy
        )
        set_event_loop_policy(WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
