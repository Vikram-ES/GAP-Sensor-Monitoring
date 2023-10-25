import paho.mqtt.client as mqtt

# You can now use the Paho MQTT library in your Python code.

clientId = 'test'
port = 1883
broker = 'localhost'

client = mqtt.Client(clientId)
client.connect(broker, port)