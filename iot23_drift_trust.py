from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.utils.class_weight import compute_sample_weight


class IoT23DriftTrustModel:
    """Binary Benign vs Defective classifier with drift/trust scores. Optimized for real-time."""

    def __init__(
        self,
        csv_path: Optional[str] = None,
        label_col: str = "label",
        benign_label: str = "Benign",
    ) -> None:
        self.csv_path = csv_path or str(Path(__file__).resolve().parent / "iot23_combined.csv")
        self.label_col = label_col
        self.benign_label = benign_label
        self._clf = self._scaler = self._le = None
        self._cols: Optional[List[str]] = None
        self._means = self._stds = None
        self._thresh = 0.5

    def train(
        self,
        test_size: float = 0.2,
        rs: int = 42,
        max_iter: int = 100,
        max_leaf_nodes: int = 31,
        defective_weight: float = 2.0,
    ) -> dict:
        """Train binary Benign/Defective model. Smaller defaults = faster predict."""
        df = pd.read_csv(self.csv_path)
        df = df.drop(columns=[c for c in df.columns if str(c).startswith("Unnamed")], errors="ignore")
        if self.label_col not in df.columns:
            raise ValueError(f"Label '{self.label_col}' not in CSV")
        y = df[self.label_col].apply(lambda v: self.benign_label if v == self.benign_label else "Defective")
        X = df.drop(columns=[self.label_col]).apply(pd.to_numeric, errors="coerce").fillna(0.0)

        self._cols = list(X.columns)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=rs, stratify=y)
        self._means = X_train.mean().values.astype(np.float64)
        self._stds = np.maximum(X_train.std().values, 1e-8).astype(np.float64)

        le = LabelEncoder()
        y_tr = le.fit_transform(y_train)
        y_te = le.transform(y_test)
        self._le = le

        scaler = StandardScaler()
        X_tr = scaler.fit_transform(X_train)
        X_te = scaler.transform(X_test)
        self._scaler = scaler

        sw = compute_sample_weight("balanced", y_tr)
        if "Defective" in le.classes_:
            idx = int(np.argmax(le.classes_ == "Defective"))
            sw = np.where(y_tr == idx, sw * defective_weight, sw)

        clf = HistGradientBoostingClassifier(max_iter=max_iter, max_leaf_nodes=max_leaf_nodes, random_state=rs)
        clf.fit(X_tr, y_tr, sample_weight=sw)
        self._clf = clf

        # Tune threshold
        if "Defective" in le.classes_:
            idx = int(np.argmax(le.classes_ == "Defective"))
            p = clf.predict_proba(X_te)[:, idx]
            best_acc, best_t = 0.0, 0.5
            for t in np.arange(0.3, 0.56, 0.05):
                pred = np.where(p >= t, idx, 1 - idx)
                acc = (pred == y_te).mean()
                if acc > best_acc:
                    best_acc, best_t = acc, float(t)
            self._thresh = best_t

        pred = self._predict_proba_to_class(clf.predict_proba(X_te))
        acc = accuracy_score(y_te, pred)
        report = classification_report(y_te, pred, target_names=le.classes_, output_dict=True)
        return {"accuracy": acc, "report": report, "classes": le.classes_.tolist()}

    def _predict_proba_to_class(self, proba: np.ndarray) -> np.ndarray:
        if "Defective" not in self._le.classes_:
            return np.argmax(proba, axis=1)
        idx = int(np.argmax(self._le.classes_ == "Defective"))
        return np.where(proba[:, idx] >= self._thresh, idx, 1 - idx).astype(np.int64)

    def predict(
        self,
        features: Union[Dict[str, Any], List[Dict[str, Any]]],
        with_drift: bool = True,
    ) -> Union[Dict, List[Dict]]:
        """
        Predict label, confidence, drift, trust. Pass dict or list of dicts.
        with_drift=False skips drift/trust for max speed (real-time).
        """
        if self._clf is None:
            raise RuntimeError("Model not trained/loaded")
        cols = self._cols
        single = isinstance(features, dict)
        if single:
            features = [features]
        if not features:
            return {} if single else []

        X = np.array([[float(f.get(c, 0.0)) for c in cols] for f in features], dtype=np.float64)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            Xs = self._scaler.transform(X)
        proba = self._clf.predict_proba(Xs)
        pred_idx = self._predict_proba_to_class(proba)
        labels = self._le.inverse_transform(pred_idx)
        conf = np.array([proba[i, pred_idx[i]] for i in range(len(pred_idx))])

        out = []
        for i in range(len(features)):
            d = {"label": str(labels[i]), "confidence": float(conf[i])}
            if with_drift:
                z = np.abs((X[i] - self._means) / self._stds)
                drift = float(np.clip(np.mean(z) / 5.0, 0.0, 1.0))
                trust = float(np.clip(conf[i] * (1.0 - drift / 2.0), 0.0, 1.0))
                d["drift_score"] = drift
                d["trust_score"] = trust
            else:
                d["drift_score"] = 0.0
                d["trust_score"] = float(conf[i])
            out.append(d)
        return out[0] if single else out

    def save(self, path: str = "models/iot23_drift_trust.joblib") -> str:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump({
            "clf": self._clf, "scaler": self._scaler, "le": self._le,
            "cols": self._cols, "means": self._means, "stds": self._stds,
            "thresh": self._thresh, "benign": self.benign_label,
        }, path)
        return path

    @classmethod
    def load(cls, path: str) -> "IoT23DriftTrustModel":
        if not os.path.exists(path):
            raise FileNotFoundError(f"Model not found: {path}")
        d = joblib.load(path)
        m = cls(benign_label=d.get("benign_label", d.get("benign", "Benign")))

        # Model core
        m._clf = d["clf"]
        m._scaler = d["scaler"]
        m._le = d.get("le") or d.get("label_encoder")
        if m._le is None:
            raise KeyError("Saved model is missing label encoder ('le' / 'label_encoder').")

        # Feature metadata
        cols = d.get("cols") or d.get("feature_columns")
        if cols is None:
            raise KeyError("Saved model is missing feature columns ('cols' / 'feature_columns').")
        m._cols = list(cols)

        # Drift stats (support old + new saved formats)
        def _first_present(*keys: str):
            for k in keys:
                if k in d and d[k] is not None:
                    return d[k]
            return None

        means = _first_present("means", "feature_means_np", "feature_means")
        stds = _first_present("stds", "feature_stds_np", "feature_stds")
        if means is None or stds is None:
            raise KeyError(
                "Saved model is missing drift stats (expected 'means/stds' or "
                "'feature_means[_np]' / 'feature_stds[_np]')."
            )
        m._means = np.asarray(means, dtype=np.float64)
        m._stds = np.asarray(stds, dtype=np.float64)

        # Decision threshold (old + new keys)
        m._thresh = float(d.get("defective_threshold", d.get("thresh", 0.5)))
        return m


def main() -> None:
    model = IoT23DriftTrustModel()
    print("Training (Benign vs Defective)...")
    m = model.train()
    print(f"Accuracy: {m['accuracy']:.4f}")
    model.save()
    print("Saved to models/iot23_drift_trust.joblib")
    df = pd.read_csv(model.csv_path)
    df = df.drop(columns=[c for c in df.columns if "Unnamed" in str(c)], errors="ignore")
    row = {c: float(df[c].iloc[0]) for c in df.columns if c != model.label_col}
    print("Sample:", model.predict(row))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)