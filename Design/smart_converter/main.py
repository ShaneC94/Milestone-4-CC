from google.cloud import pubsub_v1
import json
import os

project_id = ""

input_subscription = "filtered-sub"
output_topic = "converted-topic"

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()

subscription_path = subscriber.subscription_path(project_id, input_subscription)
topic_path = publisher.topic_path(project_id, output_topic)

print(f"Listening on {subscription_path}")

def callback(message):
    record = json.loads(message.data.decode("utf-8"))

    # Convert units
    record["pressure_psi"] = record["pressure"] / 6.895
    record["temperature_f"] = record["temperature"] * 1.8 + 32

    publisher.publish(topic_path, json.dumps(record).encode("utf-8"))
    message.ack()

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    streaming_pull_future.result()
