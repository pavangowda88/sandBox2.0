from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# drift factor that increases slowly
drift_factor = 0.0


def apply_drift(data, drift):

    drifted = {}

    for k, v in data.items():

        if isinstance(v, (int, float)):
            drifted[k] = v * (1 + drift)

        else:
            drifted[k] = v

    return drifted


def generate_benign():

    return {
        "duration": random.uniform(0.5, 5),
        "orig_bytes": random.randint(100, 1500),
        "resp_bytes": random.randint(100, 2000),
        "missed_bytes": 0.0,
        "orig_pkts": random.randint(2, 20),
        "orig_ip_bytes": random.randint(200, 3000),
        "resp_pkts": random.randint(2, 20),
        "resp_ip_bytes": random.randint(200, 3000),

        "proto_icmp": random.choice([0,0,0,1]),
        "proto_tcp": random.choice([0,1]),
        "proto_udp": random.choice([0,1]),

        "conn_state_S0": random.choice([0,1]),
        "conn_state_S1": random.choice([0,1])
    }


print("Producer running...")

while True:

    # always start from benign baseline
    base = generate_benign()

    # apply gradual drift
    data = apply_drift(base, drift_factor)

    data["drift_factor"] = drift_factor

    producer.send("network_log", data)
    producer.flush()

    print("Sent:", data)

    # increase drift slowly
    drift_factor += random.uniform(0.01, 0.03)

    # cap drift so it doesn't explode
    drift_factor = min(drift_factor, 1.0)

    time.sleep(2)