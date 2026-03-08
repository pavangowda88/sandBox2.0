import json
import time
from kafka import KafkaProducer

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "iot_packets"

benign_data = [
    {"duration": 0.12, "orig_bytes": 74, "resp_bytes": 74, "proto": "TCP", "conn_state": "SF", "orig_pkts": 1, "resp_pkts": 1},
    # {"duration": 0.0, "orig_bytes": 0, "resp_bytes": 0, "proto": "UDP", "conn_state": "S0", "orig_pkts": 1, "resp_pkts": 0},
    # {"duration": 0.25, "orig_bytes": 1500, "resp_bytes": 54, "proto": "TCP", "conn_state": "SF", "orig_pkts": 2, "resp_pkts": 1},
    # # {"duration": 0.08, "orig_bytes": 66, "resp_bytes": 66, "proto": "TCP", "conn_state": "SF", "orig_pkts": 1, "resp_pkts": 1},
    # # {"duration": 0.0, "orig_bytes": 0, "resp_bytes": 0, "proto": "UDP", "conn_state": "REJ", "orig_pkts": 1, "resp_pkts": 0},
    # # {"duration": 1.23, "orig_bytes": 1200, "resp_bytes": 800, "proto": "TCP", "conn_state": "S1", "orig_pkts": 5, "resp_pkts": 3},
    # # {"duration": 0.05, "orig_bytes": 40, "resp_bytes": 40, "proto": "TCP", "conn_state": "SF", "orig_pkts": 1, "resp_pkts": 1},
    # # {"duration": 0.0, "orig_bytes": 8, "resp_bytes": 0, "proto": "UDP", "conn_state": "S0", "orig_pkts": 1, "resp_pkts": 0},
    # # {"duration": 0.34, "orig_bytes": 500, "resp_bytes": 300, "proto": "TCP", "conn_state": "SF", "orig_pkts": 3, "resp_pkts": 2},
    # # {"duration": 2.1, "orig_bytes": 2000, "resp_bytes": 1000, "proto": "TCP", "conn_state": "SF", "orig_pkts": 8, "resp_pkts": 4},

    # {"duration": 180.5, "orig_bytes": 50000, "resp_bytes": 120, "proto": "TCP", "conn_state": "S1", "orig_pkts": 2500, "resp_pkts": 5},
    
    # # Okiru Botnet
    # {"duration": 45.2, "orig_bytes": 25000, "resp_bytes": 0, "proto": "UDP", "conn_state": "S0", "orig_pkts": 1250, "resp_pkts": 0},
    
    # # Horizontal PortScan
    # {"duration": 0.01, "orig_bytes": 1, "resp_bytes": 0, "proto": "TCP", "conn_state": "REJ", "orig_pkts": 1, "resp_pkts": 0},
    
    # # DDoS Flood
    # {"duration": 300.0, "orig_bytes": 100000, "resp_bytes": 0, "proto": "UDP", "conn_state": "S0", "orig_pkts": 5000, "resp_pkts": 0},
    
    # # Torii Malware
    # {"duration": 120.8, "orig_bytes": 80000, "resp_bytes": 200, "proto": "TCP", "conn_state": "RSTR", "orig_pkts": 4000, "resp_pkts": 10},
    
    # # PortScan Attack
    # {"duration": 0.001, "orig_bytes": 0, "resp_bytes": 0, "proto": "TCP", "conn_state": "S0", "orig_pkts": 1, "resp_pkts": 0},
    
    # # C&C Heartbeat
    # {"duration": 60.3, "orig_bytes": 1500, "resp_bytes": 1500, "proto": "TCP", "conn_state": "SF", "orig_pkts": 75, "resp_pkts": 75},
    
    # # Massive DDoS
    # {"duration": 600.0, "orig_bytes": 500000, "resp_bytes": 0, "proto": "UDP", "conn_state": "S0", "orig_pkts": 25000, "resp_pkts": 0},
    
    # # Exploit Attempt
    # {"duration": 5.2, "orig_bytes": 10000, "resp_bytes": 0, "proto": "TCP", "conn_state": "RSTO", "orig_pkts": 500, "resp_pkts": 0},
    
    # # Botnet Command
    # {"duration": 90.7, "orig_bytes": 30000, "resp_bytes": 100, "proto": "TCP", "conn_state": "S2", "orig_pkts": 1500, "resp_pkts": 5},
{"duration": 1800.0, "orig_bytes": 1000000, "resp_bytes": 0, "proto": "UDP", "conn_state": "S0", "orig_pkts": 50000, "resp_pkts": 0},
{"duration": 3600.5, "orig_bytes": 2500000, "resp_bytes": 10, "proto": "TCP", "conn_state": "S1", "orig_pkts": 125000, "resp_pkts": 1},
{"duration": 0.0001, "orig_bytes": 1, "resp_bytes": 0, "proto": "TCP", "conn_state": "REJ", "orig_pkts": 1, "resp_pkts": 0},
{"duration": 7200.0, "orig_bytes": 5000000, "resp_bytes": 0, "proto": "UDP", "conn_state": "S0", "orig_pkts": 250000, "resp_pkts": 0},
{"duration": 900.8, "orig_bytes": 200000, "resp_bytes": 5, "proto": "TCP", "conn_state": "RSTR", "orig_pkts": 10000, "resp_pkts": 1},
{"duration": 0.00001, "orig_bytes": 0, "resp_bytes": 0, "proto": "TCP", "conn_state": "S0", "orig_pkts": 1, "resp_pkts": 0},
{"duration": 5400.3, "orig_bytes": 750000, "resp_bytes": 50, "proto": "TCP", "conn_state": "S2", "orig_pkts": 37500, "resp_pkts": 2},
{"duration": 14400.0, "orig_bytes": 10000000, "resp_bytes": 0, "proto": "UDP", "conn_state": "S0", "orig_pkts": 500000, "resp_pkts": 0},
{"duration": 120.2, "orig_bytes": 50000, "resp_bytes": 0, "proto": "TCP", "conn_state": "RSTO", "orig_pkts": 2500, "resp_pkts": 0},
{"duration": 10800.7, "orig_bytes": 3000000, "resp_bytes": 20, "proto": "TCP", "conn_state": "S1", "orig_pkts": 150000, "resp_pkts": 1}
]

print("Sending benign traffic...\n")

for packet in benign_data:
    producer.send('network_log', packet)
    print("Sent:", packet)
    time.sleep(6)  # simulate real-time traffic

producer.flush()

print("\nAll packets sent.")