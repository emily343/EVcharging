
import json
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# read bootstrap and topics from common/config.json
import os
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "common", "config.json")
with open(CONFIG_PATH) as f:
    cfg = json.load(f)

bootstrap = cfg["kafka_bootstrap"]
T = cfg["topics"]

topics = [
    T["telemetry"],
    T["cp_status"],
    T["cp_events"],
    T["driver_events"],
    T["central_cmd"]
]

admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="creator")

defs = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics]

try:
    admin.create_topics(new_topics=defs, validate_only=False)
    print("Topics created:", topics)
except TopicAlreadyExistsError:
    print("Some topics already exist. OK.")
finally:
    admin.close()
