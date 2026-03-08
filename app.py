"""
Drift-aware trust monitoring web app for IoT23 model (single device).

Runs a Kafka consumer similar to final.py, computes a cumulative device trust
score based on drift, and automatically stops streaming when trust falls below 30%.

Run:
    python app.py
Then open:
    http://127.0.0.1:5000
"""

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from flask import Flask, jsonify, render_template

try:
    from kafka import KafkaConsumer
    from iot23_drift_trust import IoT23DriftTrustModel

    KAFKA_AVAILABLE = True
except Exception:
    KAFKA_AVAILABLE = False
    KafkaConsumer = None
    IoT23DriftTrustModel = None


app = Flask(__name__)

MAX_POINTS = 200
TRUST_THRESHOLD_PERCENT = 30.0  # below this triggers alert and auto-stop

results: List[Dict[str, Any]] = []
results_lock = threading.Lock()

model = None
consumer_thread: threading.Thread | None = None
stop_event = threading.Event()
consumer_error: str | None = None
cumulative_trust: float | None = None

alert_state: Dict[str, Any] = {
    "triggered": False,
    "message": "",
    "at": None,
    "last_trust": None,
}


def _reset_alert() -> None:
    """Reset session-level trust/alert state."""
    global cumulative_trust
    alert_state["triggered"] = False
    alert_state["message"] = ""
    alert_state["at"] = None
    alert_state["last_trust"] = None
    cumulative_trust = None


def _compute_trust_percent(drift_score: float) -> float:
    """Simple trust metric derived purely from drift: 100% at 0 drift, 0% at 1.0."""
    if drift_score is None:
        return 0.0
    return max(0.0, min(1.0, 1.0 - float(drift_score))) * 100.0


def run_consumer() -> None:
    """Background thread: consume from Kafka, update drift/trust timeline."""
    global model, consumer_error, cumulative_trust

    consumer_error = None
    _reset_alert()

    try:
        model_path = Path(__file__).resolve().parent / "models" / "iot23_drift_trust.joblib"
        if not model_path.exists():
            consumer_error = f"Model not found: {model_path}"
            return
        model = IoT23DriftTrustModel.load(str(model_path))
    except Exception as e:  # noqa: BLE001
        consumer_error = f"Failed to load model: {e}"
        return

    try:
        consumer = KafkaConsumer(
            "network_log",
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
        )
    except Exception as e:  # noqa: BLE001
        consumer_error = f"Kafka connection failed: {e}. Is Kafka running on localhost:9092?"
        return

    try:
        for msg in consumer:
            if stop_event.is_set():
                break

            try:
                data = msg.value

                features = {c: 0.0 for c in model._cols}
                for k, v in data.items():
                    if k in features:
                        features[k] = v

                result = model.predict(features)
                drift = float(result.get("drift_score", 0.0))
                inst_trust = _compute_trust_percent(drift)

                # Cumulative trust: monotonic, can only stay the same or go down.
                with results_lock:
                    if cumulative_trust is None:
                        cumulative_trust = inst_trust
                    else:
                        cumulative_trust = min(cumulative_trust, inst_trust)

                    entry = {
                        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "label": result.get("label"),
                        "drift_score": round(drift, 4),
                        "inst_trust": round(inst_trust, 2),
                        "trust_percent": round(cumulative_trust, 2),
                    }

                    results.append(entry)
                    if len(results) > MAX_POINTS:
                        del results[0 : len(results) - MAX_POINTS]

                if cumulative_trust is not None and cumulative_trust < TRUST_THRESHOLD_PERCENT:
                    alert_state["triggered"] = True
                    alert_state["message"] = "Trust dropped below 30%. Device might be harmful."
                    alert_state["at"] = entry["timestamp"]
                    alert_state["last_trust"] = entry["trust_percent"]
                    stop_event.set()
                    break
            except Exception as e:  # noqa: BLE001
                consumer_error = str(e)
                stop_event.set()
                break
    finally:
        try:
            consumer.close()
        except Exception:  # noqa: BLE001
            pass


def _is_streaming() -> bool:
    return consumer_thread is not None and consumer_thread.is_alive()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/state")
def api_state():
    with results_lock:
        points = list(results)
    latest = points[-1] if points else None

    return jsonify(
        {
            "points": points,
            "latest": latest,
            "streaming": _is_streaming() and not stop_event.is_set(),
            "alert": alert_state,
            "error": consumer_error,
            "trust_threshold": TRUST_THRESHOLD_PERCENT,
        }
    )


@app.route("/api/stream/start", methods=["POST"])
def api_stream_start():
    global consumer_thread

    if not KAFKA_AVAILABLE:
        return jsonify({"ok": False, "reason": "Kafka or model not available", "streaming": False})

    if _is_streaming() and not stop_event.is_set():
        return jsonify({"ok": True, "streaming": True})

    stop_event.clear()
    _reset_alert()
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()
    return jsonify({"ok": True, "streaming": True})


@app.route("/api/stream/stop", methods=["POST"])
def api_stream_stop():
    stop_event.set()
    return jsonify({"ok": True, "streaming": False})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
