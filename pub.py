import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import paho.mqtt.client as mqtt
import time



# Signal detecation logics
# Signal detecation logics
# Signal detecation logics

def getValues(tagList):
    url = "https://exactspace.co/kairosapi/api/v1/datapoints/query"
    d = {
        "metrics": [
            {
                "tags": {},
                "name": "",
                "aggregators": [
                    {
                        "name": "avg",
                        "sampling": {
                            "value": "1",
                            "unit": "minutes"
                        }
                    }
                ]
            }
        ],
        "plugins": [],
        "cache_time": 0,
        "cache_time": 0,
        "start_relative": {
        "value": "1",
        "unit": "days"
      }
    }
    finalDF = pd.DataFrame()
    for tag in tagList:
        d['metrics'][0]['name'] = tag
        res = requests.post(url=url, json=d)
        values = json.loads(res.content)
        df = pd.DataFrame(values["queries"][0]["results"][0]['values'], columns=['time', values["queries"][0]["results"][0]['name']])
        finalDF = pd.concat([finalDF, df], axis=1)

    finalDF = finalDF.loc[:, ~finalDF.columns.duplicated()]
    finalDF.dropna(subset=['time'], inplace=True)
    finalDF['time'] = pd.to_datetime(finalDF['time'], unit='ms').dt.strftime('%d-%m-%y %H:%M')

    return finalDF

tags= [
    'GAP_GAP04.PLC04.MLD2_DATA_Anode_Geometric',
    'GAP_GAP04.PLC04.MLD1_DATA_Anode_Geometric',
    'GAP_GAP03.PLC03.SCHENCK2_FEED_RATE',
    'GAP_9dfb_BallMill_Total_Power',       # Ball mill Tags
    'GAP_GAP01.PLC01._362_E200_JIT_01.PV',
    'GAP_GAP01.PLC01._362_E310_FIT_01.PV',
    'GAP_GAP01.PLC01._362_E200_ST_01.PV', # Mixer Tags
    'GAP_GAP04.PLC04.Power_M1',
    'GAP_GAP04.PLC04.Power_M2',
    'GAP_GAP04.PLC04.MLD1_DATA_Anode_Height',
    'GAP_GAP04.PLC04.MLD2_DATA_Anode_Height',
    'GAP_GAP01.PLC01._362_E290_LIT_01.PV', #Fines silo level
    'GAP_GAP01.PLC01._362_E090_LIT_01.PV', #Grains silo level
    'GAP_GAP04.PLC04.K363_K180_PIT_01_PV', #Hydraulic pressure
    'GAP_GAP04.PLC04.K050_PIT_01_PV', #Mould 2 clamping pressure
    'GAP_GAP04.PLC04.MLD1_DATA_Mixer_Total_Power', #mixer power
    'GAP_GAP04.PLC04.MLD1_DATA_Anode_Weight', # anode m1 weight
    'GAP_GAP04.PLC04.MLD2_DATA_Anode_Weight', # anode m2 weight
    'GAP_GAP04.PLC04.MLD2_DATA_Anode_Dry_Density',
    'GAP_GAP04.PLC04.MLD1_DATA_Anode_Dry_Density',
    'GAP_GAP01.PLC01._362_E020_VT_01.PV',
    'GAP_GAP01.PLC01._362_E020_MVF_01.ACTRL.AUTOSPEEDREF',
    'GAP_GAP01.PLC01._362_E015_LIT_01.PV' #rhodax tags
]

raw_data=getValues(tags)

data = raw_data.copy()

data=data[(data['GAP_GAP03.PLC03.SCHENCK2_FEED_RATE']>=5500) & (data['GAP_GAP03.PLC03.SCHENCK2_FEED_RATE']<6400)]

data = data[(data['GAP_GAP04.PLC04.MLD2_DATA_Anode_Dry_Density'] <= 1.49) & 
            (data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Dry_Density'] <= 1.49) & 
            (data['GAP_GAP04.PLC04.MLD2_DATA_Anode_Dry_Density'] > 0) & 
            (data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Dry_Density'] > 0) & 
            (data['GAP_GAP04.PLC04.MLD2_DATA_Anode_Geometric'] <= 1.69) & 
            (data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Geometric'] <= 1.69) & 
            (data['GAP_GAP04.PLC04.MLD2_DATA_Anode_Geometric'] >= 1.56) & 
            (data['GAP_GAP04.PLC04.MLD1_DATA_Anode_Geometric'] >= 1.56)]

df_filtered=data.copy()

# Convert 'time' to datetime if it's not already
df_filtered['time'] = pd.to_datetime(df_filtered['time'])

hourly_intervals = pd.date_range(start=df_filtered['time'].min(), end=df_filtered['time'].max(), freq='H')

# Lists to store z_scores for each time period
all_z_scores = []

# Find the maximum number of data points among all time periods
max_data_points = 0

for i in range(len(hourly_intervals) - 1):
    start_time = hourly_intervals[i]
    end_time = hourly_intervals[i + 1]

    # print(f'Standard Deviation of Geometric density from mould-1:{start_time} to {end_time}:')

    mask = (df_filtered['time'] >= start_time) & (df_filtered['time'] < end_time)
    data_subset = df_filtered['GAP_GAP04.PLC04.MLD2_DATA_Anode_Geometric'][mask]

    specific_number = 1.65
    data = data_subset.values

    z_scores = (data - specific_number) / np.std(data)
    
    # Store the z_scores
    all_z_scores.extend(z_scores)

    # Update the maximum number of data points
    max_data_points = max(max_data_points, len(z_scores))

# Add a new column "z_scores" to the existing DataFrame
df_filtered['z_scores'] = np.nan  # Initialize the column with NaN values

# Assign the calculated z_scores to the "z_scores" column
data_index = 0
for i in range(len(hourly_intervals) - 1):
    start_time = hourly_intervals[i]
    end_time = hourly_intervals[i + 1]
    mask = (df_filtered['time'] >= start_time) & (df_filtered['time'] < end_time)
    num_data_points = len(mask[mask])
    df_filtered.loc[mask, 'z_scores'] = all_z_scores[data_index:data_index + num_data_points]
    data_index += num_data_points


# Define the threshold for negative z-scores and the count threshold
threshold = -0.5
count_threshold = 15

# Create lists to store periods with negative and positive z-scores
negative_periods = []
positive_periods = []

# Iterate over the data and count negative z-scores for each hour
for hour in pd.date_range(start=df_filtered['time'].min(), end=df_filtered['time'].max(), freq='H'):
    mask = (df_filtered['time'] >= hour) & (df_filtered['time'] < hour + pd.Timedelta('1 hour'))
    negative_count = (df_filtered['z_scores'][mask] < threshold).sum()
    
    if negative_count > count_threshold:
        negative_periods.append(hour)
    else:
        positive_periods.append(hour)

split_negative_periods = []
split_positive_periods = []

for period in negative_periods:
    start = period
    end = start + pd.Timedelta('1 hour')
    split_negative_periods.append((start, end))

for period in positive_periods:
    start = period
    end = start + pd.Timedelta('1 hour')
    split_positive_periods.append((start, end))



# mqtt locgics
# mqtt locgics
# mqtt locgics


mqtt_broker = "localhost"
mqtt_port = 1883

client = mqtt.Client("Publisher")
client.connect(mqtt_broker, mqtt_port)

mqtt_topic = "example/topic"

i = 0  # Initialize the counter

while True:
    # text = "Hello, MQTT from pub.py! Respone: "
    # message = f"{text} {i}"

    negative_periods_json = []
    for start, end in split_negative_periods:
        period_json = {
            "start": start.strftime("%Y-%m-%d %H:%M"),
            "end": end.strftime("%Y-%m-%d %H:%M")
        }
        negative_periods_json.append(period_json)

    # Publish the list of negative periods as a JSON object
    message = json.dumps(negative_periods_json)

    client.publish(mqtt_topic, message)
    
    # Increment the counter for the next message
    i += 1
    print(message)
    time.sleep(30)
