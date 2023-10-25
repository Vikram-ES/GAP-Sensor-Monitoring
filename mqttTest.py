###########################################################
import paho.mqtt.client as paho
import timeseries as ts
import pandas as pd
import os
import time
import requests
query = ts.timeseriesquery()
import platform
version = platform.python_version().split(".")[0]
if version == "3":
  import app_config.app_config as cfg
elif version == "2":
  import app_config as cfg
config = cfg.getconfig()
pd.set_option('display.float_format', lambda x: '%.3f' % x)
data = []
import json
import numpy as np
from iapws import IAPWS97
import math

from apscheduler.schedulers.background import BackgroundScheduler

unitId = os.environ.get("UNIT_ID")
#unitId="5df8f5a57e961b7f0bccc7ed"
#unitId="5f0ff2f892affe3a28ebb1c2"
if unitId==None:
    print ("no unit id passed")
    exit()



def on_message(client, userdata, msg):

        return []

def on_log(client, userdata, obj, buff):
    print("log: " + str(buff))
    pass

def on_connect(client, obj, flags, rc):
    print("connect: " + str(rc))
    global calculations
    global topic_line
    topic_line = "u/"+unitId+"/"
    client.subscribe("u/"+unitId+"/health")





topic_line = "u/"+unitId+"/"

shouldWrite = True

Store = {}
calcMeta = {}
lastKnown={}
measureUnit={}

client = paho.Client()

port = os.environ.get("Q_PORT")
if not port:
    port = 1883
else:
    port = int(port)
print( "Running port", port)

topic_line = "u/"+unitId+"/"

BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS")
if not BROKER_ADDRESS:
	BROKER_ADDRESS = config["BROKER_ADDRESS"]

print (BROKER_ADDRESS)

client.on_log = on_log

client.username_pw_set(username="ES-MQTT", password="iota#re-mqtt39")
client.connect(BROKER_ADDRESS, port, time_out_sec)

client.on_connect = on_connect
client.on_message = on_message
client.loop_forever()