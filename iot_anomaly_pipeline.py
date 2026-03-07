import pandas as pd
import numpy as np
import sys
import socket
import json
from collections import defaultdict, deque
from river import compose, anomaly, drift, preprocessing, stream
import logging
import time

class IoTAnomalyPipeline:
    def __init__(self):
       
        self.anomaly_model = compose.Pipeline(
            preprocessing.MinMaxScaler(),
            anomaly.HalfSpaceTrees(seed=42, height=10, n_trees=10, window_size=500)
        )
        self.drift_detector = drift.ADWIN(delta=0.002)  
        self.trust_window = 100 
        
    def clean_features(self, x):
        
        x_clean = {}
        for k, v in x.items():
            try:
                x_clean[k] = float(v)
            except (ValueError, TypeError):
                continue
        return x_clean
    
    def calculate_trust_score(self, device_scores):
       
        if len(device_scores) == 0:
            return 1.0
        recent_scores = list(device_scores)[-self.trust_window:]
        avg_score = np.mean(recent_scores)
        return max(0.0, 1.0 - avg_score)  
    
    def test_with_drift_trust(self, csv_path='iot23_combined.csv'):
        print(" PRODUCTION: IoT-23 + DRIFT + TRUST SCORES")
        df = pd.read_csv(csv_path)
        
        
        bool_cols = df.select_dtypes(include=['bool']).columns
        df[bool_cols] = df[bool_cols].astype(int)
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df_numeric = df[numeric_cols].fillna(0).head(50000)
        print(f" {len(df_numeric):,} flows | {len(numeric_cols)} features")
        
        devices = defaultdict(deque)  
        total_flows = total_anomalies = drift_count = 0
        
        for idx, row in df_numeric.iterrows():
            x = self.clean_features(row.to_dict())
            if len(x) < 3:
                continue
            
            device_id = f"iot23_{idx % 50}"
            score = self.anomaly_model.score_one(x)
            self.anomaly_model.learn_one(x)
            
           
            self.drift_detector.update(score)
            if self.drift_detector.drift_detected:
                drift_count += 1
                print(f" DRIFT #{drift_count} DETECTED at flow {total_flows:,}!")
            
           
            devices[device_id].append(score)
            if len(devices[device_id]) > self.trust_window:
                devices[device_id].popleft()
            
            trust_score = self.calculate_trust_score(devices[device_id])
            total_flows += 1
            
            if score > 0.5:
                total_anomalies += 1
                if total_anomalies <= 10:  
                    print(f" ALERT: {device_id} | Score:{score:.3f} | Trust:{trust_score:.3f}")
            
            if total_flows % 1000 == 0:
                rate = total_anomalies/total_flows*100
                print(f" {total_flows:,} flows | {rate:.1f}% anomalies | {drift_count} drifts")
        
       
        print("\n" + "="*60)
        print(" DEVICE TRUST RANKING (Top 5 Riskiest)")
        print("="*60)
        for device, scores in sorted(devices.items(), 
                                   key=lambda x: self.calculate_trust_score(x[1]), reverse=True)[:5]:
            trust = self.calculate_trust_score(scores)
            print(f" {device:12} Trust:{trust:.3f} ({len(scores)} flows)")
        
        final_rate = total_anomalies/total_flows*100
        print(f"\n PRODUCTION METRICS")
        print(f"   Flows: {total_flows:,} | Anomalies: {final_rate:.1f}%")
        print(f"   Drifts detected: {drift_count}")
        print(f"   Devices monitored: {len(devices)}")
    
    def process_live_tcp_drift(self):
        print(" LIVE STREAMING: Drift + Trust Detection")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 9999))
        
        devices = defaultdict(deque)
        total_flows = total_anomalies = drift_count = 0
        buffer = b""
        
        try:
            while True:
                data = sock.recv(16384)
                if not data:
                    break
                
                buffer += data
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    flow = json.loads(line.decode('utf-8'))
                    
                    x = self.clean_features(flow)
                    if len(x) < 3:
                        continue
                    
                    device_id = flow.get('device_id', f"live_{total_flows % 50}")
                    score = self.anomaly_model.score_one(x)
                    self.anomaly_model.learn_one(x)
                    
                    
                    self.drift_detector.update(score)
                    if self.drift_detector.drift_detected:
                        drift_count += 1
                        print(f"🌊 LIVE DRIFT #{drift_count}!")
                    
                   
                    devices[device_id].append(score)
                    if len(devices[device_id]) > self.trust_window:
                        devices[device_id].popleft()
                    trust = self.calculate_trust_score(devices[device_id])
                    
                    total_flows += 1
                    if score > 0.5:
                        total_anomalies += 1
                        print(f" LIVE: {device_id} | Score:{score:.3f} | Trust:{trust:.3f}")
                    
                    if total_flows % 100 == 0:
                        rate = total_anomalies/total_flows*100
                        print(f" LIVE: {total_flows} flows | {rate:.1f}% | Drifts:{drift_count}")
                        
        except Exception as e:
            print(f" Stream complete")
        
        print(f" LIVE RESULTS: {total_anomalies/total_flows*100:.1f}% anomalies | {drift_count} drifts")

if __name__ == "__main__":
    import sys
    args = sys.argv[1:]
    
    pipeline = IoTAnomalyPipeline()
    if '--live' in args:
        pipeline.process_live_tcp_drift()
    elif '--real' in args:
        pipeline.test_with_drift_trust()
    else:
        pipeline.test_with_drift_trust()
