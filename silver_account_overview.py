# Extarct structred data from PDFs

from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Silver layer: Extract structured account overview information from parsed PDFs"
)
def silver_account_overview():
    """
    Extract auto financing statement fields from parsed PDF documents using ai_extract.
    Designed for Volvo Car Financial Services statements and similar auto loan/lease documents.
    """
    return (
        spark.read.table("bronze_parsed_pdfs")
            # First flatten the parsed content to text for better extraction
            .withColumn(
                "document_text",
                F.expr("""
                    concat_ws('\\n',
                        transform(
                            try_cast(parsed_content:document:elements AS ARRAY<VARIANT>),
                            element -> try_cast(element:content AS STRING)
                        )
                    )
                """)
            )
            .withColumn(
                "extracted_data",
                F.expr("""
                    ai_extract(
                        document_text,
                        '{
                            "account_number": {"type": "string", "description": "Account or contract number, starts with Acct# or Account"},
                            "vin_number": {"type": "string", "description": "17-character Vehicle Identification Number (VIN)"},
                            "vehicle_description": {"type": "string", "description": "Complete vehicle description including year, make and model"},
                            "statement_date": {"type": "string", "description": "Statement date"},
                            "payment_due_date": {"type": "string", "description": "Due date for payment"},
                            "amount_due": {"type": "number", "description": "Total Amount Due"},
                            "monthly_payment": {"type": "number", "description": "Monthly Payment Amount"},
                            "payoff_amount": {"type": "number", "description": "Payoff Amount if paid early"},
                            "payments_made": {"type": "integer", "description": "Number of payments already made"},
                            "payments_remaining": {"type": "integer", "description": "Number of payments remaining"},
                            "customer_name": {"type": "string", "description": "Full name of the customer"},
                            "customer_address": {"type": "string", "description": "Complete mailing address of the customer"},
                            "contact_phone": {"type": "string", "description": "Customer Care or customer service phone number"}
                        }',
                        MAP('version', '2.0', 'instructions', 'Extract auto financing information from this Volvo Car Financial Services statement. Look for labeled fields like Acct#, VIN, Due Date, etc.')
                    )
                """)
            )
            .select(
                "path",
                "modificationTime",
                F.expr("try_cast(extracted_data:account_number AS string)").alias("account_number"),
                F.expr("try_cast(extracted_data:vin_number AS string)").alias("vin_number"),
                F.expr("try_cast(extracted_data:vehicle_description AS string)").alias("vehicle_description"),
                F.expr("try_cast(extracted_data:statement_date AS string)").alias("statement_date"),
                F.expr("try_cast(extracted_data:payment_due_date AS string)").alias("payment_due_date"),
                F.expr("try_cast(extracted_data:amount_due AS decimal(15,2))").alias("amount_due"),
                F.expr("try_cast(extracted_data:monthly_payment AS decimal(15,2))").alias("monthly_payment"),
                F.expr("try_cast(extracted_data:payoff_amount AS decimal(15,2))").alias("payoff_amount"),
                F.expr("try_cast(extracted_data:payments_made AS integer)").alias("payments_made"),
                F.expr("try_cast(extracted_data:payments_remaining AS integer)").alias("payments_remaining"),
                F.expr("try_cast(extracted_data:customer_name AS string)").alias("customer_name"),
                F.expr("try_cast(extracted_data:customer_address AS string)").alias("customer_address"),
                F.expr("try_cast(extracted_data:contact_phone AS string)").alias("contact_phone")
            )
    )
