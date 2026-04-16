import pandas as pd
import json
from datetime import datetime

class TrustShieldDataValidator:
    """
    A lightweight Data Quality validator inspired by Great Expectations.
    Ensures 'Gold' layer data meets production standards before indexing.
    """
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.results = []
        self.success = True

    def expect_column_to_not_be_null(self, column):
        null_count = self.df[column].isna().sum()
        status = bool(null_count == 0)
        if not status: self.success = False
        
        self.results.append({
            "expectation": f"expect_column_values_to_not_be_null({column})",
            "success": status,
            "details": {"null_count": int(null_count)}
        })

    def expect_column_values_to_be_between(self, column, min_val, max_val):
        out_of_range = self.df[(self.df[column] < min_val) | (self.df[column] > max_val)]
        status = bool(len(out_of_range) == 0)
        if not status: self.success = False

        self.results.append({
            "expectation": f"expect_column_values_to_be_between({column}, {min_val}, {max_val})",
            "success": status,
            "details": {"out_of_range_count": len(out_of_range)}
        })

    def expect_column_values_to_be_in_set(self, column, allowed_set):
        invalid_values = self.df[~self.df[column].isin(allowed_set)]
        status = bool(len(invalid_values) == 0)
        if not status: self.success = False

        self.results.append({
            "expectation": f"expect_column_values_to_be_in_set({column})",
            "success": status,
            "details": {
                "invalid_count": len(invalid_values),
                "allowed_values": allowed_set
            }
        })

    def get_report(self):
        return {
            "timestamp": datetime.now().isoformat(),
            "overall_success": bool(self.success),
            "total_records": int(len(self.df)),
            "validations": self.results
        }

def run_quality_gate(df):
    """
    Orchestrates the validation of the Gold dataset.
    Raises Exception if validation fails to stop the Airflow pipeline.
    """
    validator = TrustShieldDataValidator(df)
    
    # Define our 'Gold' standards
    validator.expect_column_to_not_be_null("content_title")
    validator.expect_column_to_not_be_null("url")
    validator.expect_column_values_to_be_between("misinfo_probability", 0, 1)
    validator.expect_column_values_to_be_in_set("credibility_category", ["Low Risk", "Medium Risk", "High Risk"])
    
    report = validator.get_report()
    print("--- DATA QUALITY REPORT ---")
    print(json.dumps(report, indent=4))
    
    if not report["overall_success"]:
        raise ValueError("Data Quality Check FAILED. Pipeline halted to prevent bad data in Elasticsearch.")
    
    print("Data Quality Check PASSED.")
    return True
