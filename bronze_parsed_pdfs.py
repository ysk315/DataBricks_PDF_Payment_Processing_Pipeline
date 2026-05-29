## Parse PDF file in Brinze layer

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Bronze layer: Incrementally load PDF files from volume and parse with AI"
)
def bronze_pdf_raw():
    """
    Use Auto Loader to incrementally read PDF files from Unity Catalog volume
    and parse them using ai_parse_document function.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .load("/Volumes/carfindocs/carfindocs/carfindcos/")
        .selectExpr(
            "path",
            "modificationTime",
            "length",
            "ai_parse_document(content, MAP('version', '2.0')) as parsed_content"
        )
    )
