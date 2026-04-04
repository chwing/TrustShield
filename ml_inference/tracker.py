import json
from datetime import datetime
import time

class TrustShieldModelTracker:
    """
    A lightweight MLflow-like tracker for monitoring model performance and metadata.
    Logs inference runs to a local metadata store for audit trails.
    """
    def __init__(self, model_name, task):
        self.model_name = model_name
        self.task = task
        self.start_time = None
        self.end_time = None
        self.metrics = {}

    def start_run(self):
        self.start_time = time.time()
        print(f"--- MODEL RUN STARTED: {self.model_name} ({self.task}) ---")

    def log_metrics(self, df):
        """Calculates basic health metrics from the resulting dataframe."""
        self.metrics = {
            "avg_probability": float(df['misinfo_probability'].mean()),
            "median_probability": float(df['misinfo_probability'].median()),
            "high_risk_percentage": float((df['credibility_category'] == 'High Risk').sum() / len(df) * 100),
            "total_samples": len(df)
        }

    def end_run(self):
        self.end_time = time.time()
        duration = round(self.end_time - self.start_time, 2)
        
        run_data = {
            "timestamp": datetime.now().isoformat(),
            "model_name": self.model_name,
            "task": self.task,
            "duration_seconds": duration,
            "metrics": self.metrics
        }
        
        print("--- MODEL RUN COMPLETE ---")
        print(json.dumps(run_data, indent=4))
        
        # Save run log for future trend analysis
        with open("ml_inference/model_run_history.jsonl", "a") as f:
            f.write(json.dumps(run_data) + "\n")
        
        return run_data

def get_model_metadata(model_name):
    """Returns static model metadata to be attached to the dataset."""
    return {
        "model_version": "1.0.0",
        "model_engine": model_name,
        "inference_framework": "Transformers/PyTorch"
    }
