from google.cloud import pubsub_v1
import json
import time
from datetime import datetime
from random import random
import pandas as pd

# TODO(developer)

project_id = "testproject-354901"
topic_id = "shipments"
max_messages = 5000
order_tracking_mock_data = pd.read_csv("order_tracking_mock.csv")

publisher = pubsub_v1.PublisherClient()
# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

i = 0
while i < max_messages:
    data = {
        "sensor_id": order_tracking_mock_data["sensor_id"][i],
        "datetime": str(order_tracking_mock_data["datetime"][i]),
        "timezone": order_tracking_mock_data["timezone"][i],
        "longitude": str(order_tracking_mock_data["longitude"][i]),
        "latitude": str(order_tracking_mock_data["latitude"][i]),
        "order_id": order_tracking_mock_data["order_id"][i],
        "vehicle_id": order_tracking_mock_data["vehicle_id"][i]
    }
    publisher.publish(topic_path, json.dumps(data).encode("utf-8"))
    time.sleep(random())
    print(json.dumps(data))
    i += 1

print(f"Published messages to {topic_path}.")