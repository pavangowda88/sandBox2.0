from scapy.all import sniff, IP, TCP, UDP
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

flows = {}
SEND_INTERVAL = 2
last_sent_time = 0


def process_packet(packet):

    global last_sent_time

    if not packet.haslayer(IP):
        return

    src = packet[IP].src
    dst = packet[IP].dst
    proto = packet[IP].proto
    size = len(packet)

    sport = 0
    dport = 0

    if packet.haslayer(TCP):
        sport = packet[TCP].sport
        dport = packet[TCP].dport

    elif packet.haslayer(UDP):
        sport = packet[UDP].sport
        dport = packet[UDP].dport

    flow_key = (src, dst, sport, dport, proto)

    now = time.time()

    if flow_key not in flows:
        flows[flow_key] = {
            "start_time": now,
            "packet_count": 0,
            "byte_count": 0
        }

    flow = flows[flow_key]

    flow["packet_count"] += 1
    flow["byte_count"] += size

    flow_duration = now - flow["start_time"]

    data = {
        "src_ip": src,
        "dst_ip": dst,
        "protocol": proto,
        "packet_size": size,
        "packet_rate": flow["packet_count"] / max(flow_duration, 1),
        "flow_duration": flow_duration,
        "packet_count": flow["packet_count"],
        "byte_count": flow["byte_count"]
    }

    # Inject artificial anomaly (10%)
    if random.random() < 0.1:
        data["byte_count"] *= 20
        data["packet_count"] *= 10
        data["flow_duration"] *= 5

    if now - last_sent_time >= SEND_INTERVAL:

        producer.send("network_log", data)
        producer.flush()

        print("Sent:", data)

        last_sent_time = now


sniff(
    iface="Intel(R) Wi-Fi 6 AX201 160MHz",
    prn=process_packet,
    store=False
)