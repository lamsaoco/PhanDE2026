import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

# Topic to read from (defaults to the topic used by the producer)
TOPIC = os.environ.get("TOPIC", "green-trips")

# Kafka broker used in this workspace
BOOTSTRAP_SERVERS = ["localhost:9092"]

# How long we’ll keep polling for additional messages when no new data arrives.
# Once this many sequential empty polls happen, we assume we’ve reached the end of the topic
# (or there is no data right now) and we exit.
MAX_EMPTY_POLLS = 5

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="count-trip-distance",
    value_deserializer=ride_deserializer,
)

print(f"Consuming topic {TOPIC} from beginning (auto_offset_reset=earliest)")

count_gt_5 = 0
empty_polls = 0

while True:
    records = consumer.poll(timeout_ms=1000)
    if not records:
        empty_polls += 1
        if empty_polls >= MAX_EMPTY_POLLS:
            break
        continue

    empty_polls = 0
    for tp, msgs in records.items():
        for msg in msgs:
            ride = msg.value
            if ride.trip_distance > 5.0:
                count_gt_5 += 1

print(f"Trips with trip_distance > 5.0: {count_gt_5}")
consumer.close()
