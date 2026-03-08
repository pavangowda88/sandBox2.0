from kafka import KafkaConsumer
import json
from iot23_drift_trust import IoT23DriftTrustModel

model = IoT23DriftTrustModel.load("models/iot23_drift_trust.joblib")

consumer = KafkaConsumer(
    "network_log",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest"
)

print("Consumer started")

for msg in consumer:

    data = msg.value

    features = {c:0.0 for c in model._cols}

    for k,v in data.items():
        if k in features:
            features[k] = v

    result = model.predict(features)

    # print("\nINPUT")
    # print(data)

    print("\nMODEL OUTPUT")
    print(result)

    print("------------------------------------------------")