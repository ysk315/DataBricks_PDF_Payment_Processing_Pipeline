# DataBricks_PDF_Payment_Processing_Pipeline
This repository   provides code to convert PDF documents into structured data.  Loan statements which are in PDF formats are  parsed and required columns are extracted and loaded into tables for further analysis.

High level steps to process and create Spark declarative pipelin 

1. To Process PDF documents, around 41 files are uploaded into databricks catalog and parsed using python.
2.  Raw data is stored under workspace.default.bronze_pdf_raw
3.  Parsed data is stored under workspace.default.silver_payment_details
4.  Cleaned and fomatted data is stored under workspace.default.gold_payment_details_masked
5.  A final table workspace.default.gold_payment_summary is used to store summarized data for further calculations
