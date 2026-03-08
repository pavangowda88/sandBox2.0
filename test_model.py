import joblib

print("Loading model...")

model = joblib.load("iot_anomaly_model.pkl")

print("Model loaded")