import paho.mqtt.client as mqtt

mqtt_broker = "localhost"
mqtt_port = 1883

client = mqtt.Client("Subscriber")

def on_message(client, userdata, message):
    print(f"Received message '{message.payload.decode()}' on topic '{message.topic}'")

client.on_message = on_message

client.connect(mqtt_broker, mqtt_port)
mqtt_topic = "example/topic"
client.subscribe(mqtt_topic)
client.loop_forever()
