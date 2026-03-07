import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import accuracy_score
from scipy.stats import ks_2samp
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')

class IoTAnomalyDetector:
    def __init__(self, df=None):
        self.df = df  # preprocessed IoT-23 dataframe with features ready
        self.model = None
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
        self.drift_reference = None
        self.feature_names = None
        self.is_encoder_fitted = False

    def _add_predictive_features(self, df):
        """Add rolling/predictive features per device/IP"""
       
        df = df.copy()
        numeric_cols = ['duration', 'orig_bytes', 'resp_bytes']
        window = 5
        
        for col in numeric_cols:
            df[f'{col}_rolling_mean'] = df.groupby('src_ip')[col].transform(lambda x: x.rolling(window, min_periods=1).mean())
            df[f'{col}_rolling_max'] = df.groupby('src_ip')[col].transform(lambda x: x.rolling(window, min_periods=1).max())
        
       
        df['recent_anomaly_count'] = df.groupby('src_ip')['label'].transform(
            lambda x: x.eq('Anomaly').rolling(window, min_periods=1).sum()
        )
        return df

    def _preprocess(self, df):
        """Encode categorical columns, add predictive features"""
        df = self._add_predictive_features(df)
        
        
        if not self.is_encoder_fitted:
            self.label_encoder.fit(df['label'])
            self.is_encoder_fitted = True
        
        
        categorical_cols = ['proto', 'conn_state']
        df_encoded = pd.get_dummies(df, columns=categorical_cols)
        
        
        X = df_encoded.drop(columns=['label', 'src_ip'], errors='ignore')
        y = self.label_encoder.transform(df['label'])
        
        if self.feature_names is None:
            self.feature_names = X.columns.tolist()
        
        return X, y

    def train(self):
        X, y = self._preprocess(self.df)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        X_train_scaled = self.scaler.fit_transform(X_train)
        self.drift_reference = X_train_scaled.copy()
        self.model = lgb.LGBMClassifier(
            n_estimators=200, max_depth=5, learning_rate=0.1,
            class_weight='balanced', random_state=42
        )
        self.model.fit(X_train_scaled, y_train)
        
        print(f"Train Acc: {accuracy_score(y_train, self.model.predict(X_train_scaled)):.2%}")
        print(f"Test Acc: {accuracy_score(y_test, self.model.predict(self.scaler.transform(X_test))):.2%}")

    def test_batch(self, df_batch):
        """Predict batch with predictive features"""
        X_batch, y_batch = self._preprocess(df_batch)
        X_scaled = self.scaler.transform(X_batch)
        preds = self.model.predict(X_scaled)
        probs = self.model.predict_proba(X_scaled)
        
        for i in range(len(df_batch)):
            raw_prob_benign = probs[i][self.label_encoder.transform(['Benign'])[0]]
            drift = self.get_drift_score(X_batch.iloc[[i]])
            trust_benign = raw_prob_benign * (1 - drift) * 100
            confidence = max(probs[i]) * (1 - drift * 0.5) * 100
            safety, action = self.get_safety_status(trust_benign, drift)
            print(f"ID:{i+1} Pred:{self.label_encoder.inverse_transform([preds[i]])[0]} Trust:{trust_benign:.1f}% Safety:{safety} Action:{action}")


if __name__ == "__main__":
    main()
