from flask import Flask, render_template, jsonify
import paho.mqtt.client as mqtt

app = Flask(__name__)

# MQTT broker settings
mqtt_broker = "localhost"
mqtt_port = 1883

# Create an MQTT client for the Flask application
mqtt_client = mqtt.Client("FlaskMQTTApp")
mqtt_client.connect(mqtt_broker, mqtt_port)

# Define a global variable to store the last received message
last_received_message = None

# Define a callback to handle incoming MQTT messages
def on_message(client, userdata, message):
    global last_received_message
    last_received_message = message.payload.decode()

# Set the callback
mqtt_client.on_message = on_message

# Subscribe to the MQTT topic
mqtt_topic = "example/topic"
mqtt_client.subscribe(mqtt_topic)
mqtt_client.loop_start()

@app.route('/')
def index():
    return render_template('index.html', data=None)

@app.route('/latest_message', methods=['GET'])
def get_latest_message():
    global last_received_message
    return jsonify({'message': last_received_message})

if __name__ == '__main__':
    app.run(debug=True)