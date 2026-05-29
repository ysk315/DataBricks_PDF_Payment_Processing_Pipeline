# Extarct structred data from PDFs

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType
import re

# UDF to parse HTML table content
def parse_html_table(html_content):
    """
    Parse HTML table content to extract field-value pairs.
    Extracts payment details from table rows.
    """
    if not html_content or '<table>' not in html_content:
        return []
    
    # Extract table rows
    rows = re.findall(r'<tr>(.*?)</tr>', html_content, re.DOTALL)
    
    field_value_pairs = []
    for row in rows:
        # Extract cells from each row
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        
        # Clean cell content (remove HTML tags)
        cells = [re.sub(r'<[^>]+>', '', cell).strip() for cell in cells]
        
        # Skip empty rows
        if len(cells) < 2:
            continue
        
        # First cell is the field name, second cell is the value
        field = cells[0]
        value = cells[1] if len(cells) > 1 else ""
        
        if field and value:
            field_value_pairs.append((field, value))
    
    return field_value_pairs

# Register UDF
field_value_schema = ArrayType(StructType([
    StructField("field", StringType(), True),
    StructField("value", StringType(), True)
]))
parse_html_table_udf = F.udf(parse_html_table, field_value_schema)

@dp.table(
    comment="Silver layer: Extract payment details from parsed PDF elements"
)
def silver_payment_details():
    """
    Extract elements from parsed PDFs, filter for table elements,
    parse HTML to get field-value pairs, and extract filename.
    """
    # List of desired payment fields
    desired_fields = [
        'Received on',
        'Received Amount',
        'Principal',
        'Finance Charge',
        'Late Charge',
        'Other',
        'Monthly Payment Amount',
        'Miscellaneous Charge(s)',
        'Past Due Amount',
        'Late Charge(s)',
        'Credit to Monthly Payment Amount',
        'Total Amount Due',
        'Due Date'
    ]
    
    return (
        spark.readStream.table("bronze_pdf_raw")
        # Extract elements array from parsed_content
        .selectExpr(
            "path",
            "explode(try_cast(parsed_content:document:elements AS array<variant>)) as element"
        )
        # Extract element fields
        .selectExpr(
            "path",
            "try_cast(element:type AS string) as element_type",
            "try_cast(element:content AS string) as content"
        )
        # Filter only table elements
        .filter(F.col("element_type") == "table")
        # Parse HTML table content using UDF
        .withColumn("field_value_pairs", parse_html_table_udf(F.col("content")))
        # Explode field-value pairs
        .select(
            F.col("path"),
            F.explode(F.col("field_value_pairs")).alias("pair")
        )
        .select(
            F.col("path"),
            F.col("pair.field").alias("field"),
            F.col("pair.value").alias("value")
        )
        # Filter only desired fields
        .filter(F.col("field").isin(desired_fields))
        # Extract filename from path
        .withColumn(
            "filename",
            F.regexp_extract(F.col("path"), r"([^/]+\.pdf)$", 1)
        )
        # Add processing timestamp
        .withColumn("processed_timestamp", F.current_timestamp())
        # Select final columns
        .select(
            "filename",
            "field",
            "value",
            "processed_timestamp",
            "path"
        )
    )
