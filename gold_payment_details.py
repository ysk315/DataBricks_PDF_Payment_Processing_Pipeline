## Load parsed elements into a gold layer table

from pyspark import pipelines as dp

@dp.materialized_view(
    comment="Gold layer: Long format payment details with one row per field per file"
)
def gold_payment_details_masked():
    """
    Simple select from silver layer to create the long format gold table.
    This table has one row for each payment field in each PDF file.
    """
    return (
        spark.read.table("silver_payment_details")
        .select(
            "filename",
            "field",
            "value",
            "processed_timestamp",
            "path"
        )
    )
