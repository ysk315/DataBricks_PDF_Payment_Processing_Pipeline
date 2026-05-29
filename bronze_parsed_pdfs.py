## Parse PDF file in Brinze layer

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Bronze layer: Parse PDF files from Volume using ai_parse_document"
)
def bronze_parsed_pdfs():
    """
    Read PDF files from /Volumes/carfindocs/carfindocs/carfindcos and parse them.
    Only processes successfully parsed documents (filters out parsing errors).
    """
    return (
        spark.read.format("binaryFile")
            .load("/Volumes/carfindocs/carfindocs/carfindcos")
            .withColumn(
                "parsed_content",
                F.expr("ai_parse_document(content, MAP('version', '2.0'))")
            )
            # Filter out documents that failed to parse
            .where("try_cast(parsed_content:error_status AS STRING) IS NULL")
            .select(
                "path",
                "modificationTime",
                "length",
                "parsed_content"
            )
    )
