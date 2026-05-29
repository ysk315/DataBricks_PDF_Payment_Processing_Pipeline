#### Summarize data

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold layer: Wide format payment summary with pivoted payment fields",
    cluster_by=["filename"]
)
def gold_payment_summary():
    """
    Pivot silver layer data to create wide format table with one row per PDF file
    and columns for each payment field.
    """
    return (
        spark.read.table("silver_payment_details")
        .groupBy("filename", "path")
        .agg(
            F.max(F.when(F.col("field") == "Received on", F.col("value"))).alias("received_on"),
            F.max(F.when(F.col("field") == "Received Amount", F.col("value"))).alias("received_amount"),
            F.max(F.when(F.col("field") == "Principal", F.col("value"))).alias("principal"),
            F.max(F.when(F.col("field") == "Finance Charge", F.col("value"))).alias("finance_charge"),
            F.max(F.when(F.col("field") == "Late Charge", F.col("value"))).alias("late_charge"),
            F.max(F.when(F.col("field") == "Other", F.col("value"))).alias("other"),
            F.max(F.when(F.col("field") == "Monthly Payment Amount", F.col("value"))).alias("monthly_payment_amount"),
            F.max(F.when(F.col("field") == "Miscellaneous Charge(s)", F.col("value"))).alias("miscellaneous_charges"),
            F.max(F.when(F.col("field") == "Past Due Amount", F.col("value"))).alias("past_due_amount"),
            F.max(F.when(F.col("field") == "Late Charge(s)", F.col("value"))).alias("late_charges"),
            F.max(F.when(F.col("field") == "Credit to Monthly Payment Amount", F.col("value"))).alias("credit_to_monthly_payment"),
            F.max(F.when(F.col("field") == "Total Amount Due", F.col("value"))).alias("total_amount_due"),
            F.max(F.when(F.col("field") == "Due Date", F.col("value"))).alias("due_date"),
            F.max(F.col("processed_timestamp")).alias("processed_timestamp")
        )
        .select(
            "filename",
            "received_on",
            "received_amount",
            "principal",
            "finance_charge",
            "late_charge",
            "other",
            "monthly_payment_amount",
            "miscellaneous_charges",
            "past_due_amount",
            "late_charges",
            "credit_to_monthly_payment",
            "total_amount_due",
            "due_date",
            "processed_timestamp",
            "path"
        )
    )
