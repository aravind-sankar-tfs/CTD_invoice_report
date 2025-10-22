# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview
# MAGIC
# MAGIC This notebook aggregates invoice line and header data from multiple tables to produce a cleaned, joined, and enriched dataset for AR reporting.
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `oas_docline_df`: Invoice line details
# MAGIC - `oas_dochead_df`: Invoice header details
# MAGIC - `oas_company_df`: Company metadata
# MAGIC - `oas_el2_element_df`: Element 2 metadata
# MAGIC - `oas_el3_element_df`: Element 3 (customer) metadata
# MAGIC
# MAGIC **Purpose:**
# MAGIC - Filter and join invoice lines and headers for relevant AR transactions
# MAGIC - Enrich with company, and element metadata
# MAGIC - Calculate home and document currency values
# MAGIC - Assign global identifiers, invoice numbers, PO, protocol, and job numbers
# MAGIC - Exclude unwanted document codes and transactions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC #### Import Libraries

# COMMAND ----------

# DBTITLE 1,Importing Libraries
# Import Spark SQL functions for DataFrame transformations
from pyspark.sql import functions as F
import logging
import msal
import requests
import io
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Utility Functions:
# MAGIC #### Functions Overview
# MAGIC
# MAGIC **1. `build_url(driveId, path, fileName)`**  
# MAGIC Constructs a Microsoft Graph API URL to download a file from SharePoint using the drive ID, file path, and file name.
# MAGIC
# MAGIC **2. `get_token(secret, clientId)`**  
# MAGIC Obtains an Azure AD access token using the provided client secret and client ID via MSAL for authenticating with Microsoft Graph API.
# MAGIC
# MAGIC **3. `get_sharepoint_excel_df(weburl, file, client_id, client_secret, driver_id, skip_num_rows, sheet_prefix="Month")`**  
# MAGIC - Downloads an Excel file (.xlsx or .xlsb) from SharePoint using Microsoft Graph API.  
# MAGIC - Validates the file type and content.  
# MAGIC - Loads the specified sheet (with a given prefix) into a pandas DataFrame, skipping a specified number of rows.  
# MAGIC - Returns the DataFrame.
# MAGIC
# MAGIC **4. `get_sharepoint(weburl, file, client_id, client_secret, driver_id, s3_bucket="s3://tfsdl-lslpg-fdt-test")`**  
# MAGIC - Downloads a file from SharePoint using Microsoft Graph API.  
# MAGIC - Saves the file locally.  
# MAGIC - Uploads the file to a specified S3 bucket.  
# MAGIC - Returns the S3 path if successful, otherwise returns None.

# COMMAND ----------

# Build Microsoft Graph API URL for SharePoint file download
def build_url(driveId, path, fileName):
    prefix = "https://graph.microsoft.com/v1.0/drives/"
    return f"{prefix}{driveId}{path}{fileName}:/content"

# Get Azure AD access token using MSAL
def get_token(secret, clientId):
    authority_url = "https://login.microsoftonline.com/b67d722d-aa8a-4777-a169-ebeb7a6a3b67"
    app = msal.ConfidentialClientApplication(
        authority=authority_url,
        client_id=clientId,
        client_credential=secret
    )
    token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    return token

logger = logging.getLogger(__name__)

# Download Excel file from SharePoint and return as pandas DataFrame
def get_sharepoint_excel_df(weburl, file, client_id, client_secret, driver_id, skip_num_rows, sheet_prefix="Month"):
    try:
        # Get access token
        token = get_token(client_secret, client_id)["access_token"]

        # Build download URL
        download_url = build_url(driveId=driver_id, path=weburl, fileName=file)
        response = requests.get(
            download_url,
            allow_redirects=True,
            headers={'Authorization': f'Bearer {token}'}
        )

        # Check for successful response
        if response.status_code != 200:
            logger.error(f"[{response.status_code}] Failed to download file from SharePoint: {download_url}")
            logger.info("SharePoint site may be inaccessible. We are working to resolve it and will keep you updated.")
            raise ValueError(f"Failed to download file: {response.status_code}")

        content_type = response.headers.get('Content-Type', '')
        file_extension = file.lower().split('.')[-1]

        # Validate file format and content type
        if file_extension == 'xlsx':
            if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' not in content_type:
                logger.error(f"Input file '{file}' is unavailable in .xlsx format. Detected content type: {content_type}")
                raise ValueError(f"Unexpected content type for .xlsx file: {content_type}")
            engine = 'openpyxl'
        elif file_extension == 'xlsb':
            if 'application/vnd.ms-excel.sheet.binary.macroEnabled.12' not in content_type:
                logger.error(f"Input file '{file}' is unavailable in .xlsb format. Detected content type: {content_type}")
                raise ValueError(f"Unexpected content type for .xlsb file: {content_type}")
            engine = 'pyxlsb'
        else:
            raise ValueError("Unsupported file format. Only .xlsx and .xlsb are supported.")

        # Load Excel file into pandas
        excel_file = pd.ExcelFile(io.BytesIO(response.content), engine=engine)
        sheet_name = next((s for s in excel_file.sheet_names if s.startswith(sheet_prefix)), None)

        # Check if the required sheet exists
        if not sheet_name:
            logger.error(f"No sheet starting with '{sheet_prefix}' found in the file '{file}'.")
            raise ValueError(f"No sheet starting with '{sheet_prefix}' found in the file.")

        # Read the specified sheet into a DataFrame
        df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skip_num_rows, engine=engine)

        return df

    except Exception as e:
        logger.exception(
            f"Input file '{file}' is unavailable in the SharePoint site or cannot be read. Please check from your side and share the input files."
        )
        raise

# Download file from SharePoint and upload to S3 bucket
def get_sharepoint(weburl, file, client_id, client_secret, driver_id, s3_bucket="s3://tfsdl-lslpg-fdt-test"):
    try:
        # Get access token
        api_key = get_token(client_secret, client_id)["access_token"]
        # Build download URL
        download_url = build_url(driveId=driver_id, path=weburl, fileName=file)
        readSharepoint = requests.get(
            download_url,
            allow_redirects=True,
            headers={'Authorization': api_key}
        )

        # Save file locally
        local_path = f"/tmp/{file.replace(' ', '_')}"
        with open(local_path, "wb") as fptr:
            fptr.write(readSharepoint.content)

        # Upload file to S3
        s3_path = f"{s3_bucket}/{file.replace(' ', '_').replace('.xlsx','')}"
        dbutils.fs.cp(f"file://{local_path}", s3_path)
        print(s3_path)
    except FileNotFoundError as e:
        logger.warning(f"{e} Download failed")
        return None
    else:
        if readSharepoint.status_code == 200:
            logger.info(f"HTTP {readSharepoint.status_code}: File successfully downloaded from SharePoint and uploaded to S3")
            return s3_path  # S3 path returned
        else:
            logger.warning(f"HTTP {readSharepoint.status_code}: Unable to download file from SharePoint")
            return None

# COMMAND ----------

# MAGIC %md
# MAGIC #### Declare Variables

# COMMAND ----------

# DBTITLE 1,Declare Variables and Assign Strings
# S3 path for OAS document line Delta table
oas_docline_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_docline"

# S3 path for OAS document head Delta table
oas_dochead_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_dochead"

# S3 path for OAS company Delta table
oas_company_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_company"

# S3 path for OAS EL3 element Delta table
oas_el2_element_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el2_element"

# S3 path for OAS EL3 element Delta table
oas_el3_element_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el3_element"

# Set the S3 path for Databricks data
temp = "s3a://tfsdl-lsg-spot-test/" + f"databricks/data"

# Credentials and connection string
fids_all_user = 'FIN_DIGI_TF_DEV_ETL_USER'
fids_all_password = dbutils.secrets.get('ds-crg-fdt', 'fin_dev')  # Retrieve password from Databricks secrets
fids_all_url = (
    f"jdbc:snowflake://ppd.us-east-1.snowflakecomputing.com:443/EDWD"
    f"?user={fids_all_user}&password={fids_all_password}"
)

# SharePoint and Azure app credentials
client_id = 'ffdd77e0-b8f0-4c02-879e-51c41bbcd0fa'
client_secret = dbutils.secrets.get('ds-lsg-fids', 'fids_azure_sharepoint_app_secret')  
# Get client secret from secrets
driver_id = dbutils.secrets.get('ds-lsg-fids', 'fids_sharepoint_driverID') + '/root:'  # SharePoint driver ID
token = get_token(client_secret, client_id)["access_token"]  # Get access token using client credentials

# SharePoint web URL path
weburl = "/CTD/"

# SharePoint lookup file for CODA Reference Correction
file_name_mapEntity = "MAP-Entity.xlsx"
file_name_zInvoiceCode = "z_InvoicingDcode.xlsx"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Input Datasets

# COMMAND ----------

# DBTITLE 1,Create Input Datasets
# Load OAS document line Delta table from S3
oas_docline_df = (
    spark.read.format("delta")
    .load(oas_docline_str))

# Load OAS document head Delta table from S3
oas_dochead_df = spark.read.format("delta").load(oas_dochead_str)

# Load OAS company Delta table from S3 
oas_company_df = spark.read.format("delta").load(oas_company_str)

# Load OAS EL2 element Delta table from S3 
oas_el2_element_df = spark.read.format("delta").load(oas_el2_element_str) 

# Load OAS EL3 element Delta table from S3 
oas_el3_element_df = spark.read.format("delta").load(oas_el3_element_str)    

# Download SharePoint Excel file and load as pandas DataFrame
mapEntity_df = get_sharepoint_excel_df(
            weburl + '/', file_name_mapEntity, client_id, client_secret, driver_id,
            skip_num_rows=0, sheet_prefix="Sheet1")
# Convert pandas DataFrame to Spark DataFrame for coda_ref_correction_df
mapEntity_df = spark.createDataFrame(mapEntity_df)

# Download SharePoint Excel file and load as pandas DataFrame
zInvoiceCode_df = get_sharepoint_excel_df(
            weburl + '/', file_name_zInvoiceCode, client_id, client_secret, driver_id,
            skip_num_rows=0, sheet_prefix="Sheet1")
# Convert pandas DataFrame to Spark DataFrame for coda_ref_correction_df
zInvoiceCode_df = spark.createDataFrame(zInvoiceCode_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### arlines_df
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Join `oas_docline_df` and `oas_dochead_df` to extract AR line items for specific entities and codes, filtered by payment status and date.
# MAGIC
# MAGIC **Input datasets:**  
# MAGIC - `oas_docline_df` (document line details)  
# MAGIC - `oas_dochead_df` (document header details)
# MAGIC
# MAGIC **Output dataset:**  
# MAGIC - `arlines_df` (Spark DataFrame with columns: `cmpcode`, `doccode`, `docnum`, `el3`)

# COMMAND ----------

# Join oas_docline_df and oas_dochead_df on cmpcode, docnum, and doccode
# Filter for AR transactions with el2 starting with '021%' or specific el2 for certain companies
# Exclude statpay 665 and restrict to docdate >= 2019-01-01
# Select relevant columns for downstream joins
arlines_df = (
    oas_docline_df.alias("DL")
    .join(
        oas_dochead_df.alias("DH"),
        (F.trim(F.col("DL.cmpcode")) == F.trim(F.col("DH.cmpcode"))) &
        (F.trim(F.col("DL.docnum")) == F.trim(F.col("DH.docnum"))) &
        (F.trim(F.col("DL.doccode")) == F.trim(F.col("DH.doccode"))),
        "inner"
    )
    .filter(
        (F.col("DL.el2").like("021%")) |
        (
            F.col("DL.cmpcode").isin("ALLENTOWN", "MP", "CLINTRAK", "INDY") &
            (F.col("DL.el2") == "28020")
        )
    )
    .filter(F.col("DL.statpay") != 665)
    .filter(F.col("DH.docdate") >= F.lit("2019-01-01"))
    .filter(F.col("DH.cdc_operation_type") != 'D') 
    .select("DL.cmpcode", "DL.doccode", "DL.docnum", "DL.el3")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### base_df
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Join `oas_docline_df` (document line details) and `oas_dochead_df` (document header details) to enrich line-level data with header-level information, and derive new columns (`global_identifier`, `invoice_number`, `po_value`, `protocol_value`) based on business logic.
# MAGIC
# MAGIC **Input datasets:**  
# MAGIC - `oas_docline_df` (document line details)  
# MAGIC - `oas_dochead_df` (document header details)
# MAGIC
# MAGIC **Output dataset:**  
# MAGIC - `base_df` (Spark DataFrame with joined and derived columns)

# COMMAND ----------

# Join oas_docline_df and oas_dochead_df on cmpcode, doccode, and docnum
# Select all columns from document line, plus header docdate and curdoc
# Derive global_identifier, invoice_number, po_value, and protocol_value based on business logic
base_df = (
    oas_docline_df.alias("DL2")
    .join(
        oas_dochead_df.alias("DH"),
        (F.trim(F.col("DL2.cmpcode")) == F.trim(F.col("DH.cmpcode"))) &
        (F.trim(F.col("DL2.doccode")) == F.trim(F.col("DH.doccode"))) &
        (F.trim(F.col("DL2.docnum")) == F.trim(F.col("DH.docnum"))),
        "inner"
    )
    .select(
        *[F.col(f"DL2.{c}") for c in oas_docline_df.columns],  # All columns from document line
        F.col("DH.docdate"),  # Document date from header
        F.col("DH.curdoc"),   # Document currency from header
        # Global identifier: company code + business logic for ref1/ref5/docnum
        F.concat(
            F.upper(F.col("DL2.cmpcode")),
            F.lit("-"),
            F.when(F.col("DL2.cmpcode").isin("ALLENTOWN", "MAI", "CHINA", "SINGAPORE", "CLINTRAK", "MP"), F.col("DL2.ref1"))
             .when(F.col("DL2.cmpcode").isin("JAPAN", "SUZHOU"), F.col("DL2.ref5"))
             .otherwise(F.ltrim(F.col("DL2.docnum")))
        ).alias("global_identifier"),
        # Invoice number: business logic by company and ref fields
        (
            F.when(F.col("DH.cmpcode").isin("ALLENTOWN", "MAI", "CHINA", "CLINTRAK", "MP", "INDY"), F.col("DL2.ref1"))
             .when(F.col("DH.cmpcode").isin("BAS", "WAR", "HEG"), F.ltrim(F.col("DH.docnum").cast("string")))
             .when((F.col("DH.cmpcode") == "SINGAPORE") & (F.col("DL2.ref1").like("CN%")), F.expr("substring(DL2.ref1, 3, length(DL2.ref1)-2)"))
             .when(F.col("DH.cmpcode").isin("KOREA", "JAPAN", "SUZHOU"), F.col("DL2.ref1"))
             .otherwise(F.lit("Unknown"))
        ).alias("invoice_number"),
        # PO value: business logic by company and docdate
        (
            F.when(F.col("DH.cmpcode") == "SUZHOU", F.col("DL2.ref2"))
             .when((F.col("DH.cmpcode") == "WAR") & (F.col("DH.docdate") >= F.lit("2020-11-10")), F.col("DL2.ref3"))
             .when((F.col("DH.cmpcode") == "KOREA") & (F.col("DH.docdate") <= F.lit("2024-07-12")), F.col("DL2.ref2"))
             .when((F.col("DH.cmpcode") == "KOREA") & (F.col("DH.docdate") == F.lit("2024-07-23")), F.col("DL2.ref3"))
             .when((F.col("DH.cmpcode") == "JAPAN") & ((F.col("DH.docdate") <= F.lit("2024-07-31")) | (F.col("DH.docdate") == F.lit("2024-08-20"))), F.col("DL2.ref2"))
             .otherwise(F.col("DL2.ref4"))
        ).alias("po_value"),
        # Protocol value: business logic by company and docdate
        (
            F.when(F.col("DH.cmpcode") == "SUZHOU", F.col("DL2.ref3"))
             .when((F.col("DH.cmpcode") == "KOREA") & (F.col("DH.docdate") <= F.lit("2024-07-12")), F.col("DL2.ref3"))
             .when((F.col("DH.cmpcode") == "JAPAN") & ((F.col("DH.docdate") <= F.lit("2024-07-31")) | (F.col("DH.docdate") == F.lit("2024-08-20"))), F.col("DL2.ref3"))
             .otherwise(F.col("DL2.ref2"))
        ).alias("protocol_value")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### jobdefaults_df
# MAGIC
# MAGIC For each `cmpcode` in the `base` DataFrame, find the non-null, non-empty `el4` value with the earliest `docdate`.
# MAGIC
# MAGIC **Input:**  
# MAGIC - `base` (Spark DataFrame)
# MAGIC
# MAGIC **Output:**  
# MAGIC - `jobdefaults_df` (Spark DataFrame with columns `cmpcode` and `default_el4`)

# COMMAND ----------

# For each cmpcode, find the non-null, non-empty el4 value with the earliest docdate
jobdefaults_df = (
    base_df
    .filter(F.col("el4").isNotNull() & (F.col("el4") != ""))  # Exclude null or empty el4
    .groupBy("cmpcode")  # Group by company code
    .agg(
        F.expr("min_by(el4, docdate)").alias("default_el4")  # Get el4 with earliest docdate
    )
    .select("cmpcode", "default_el4")  # Select cmpcode and default_el4 for output
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### final_agg_df
# MAGIC
# MAGIC This notebook aggregates financial data by joining multiple tables, applying filters, and calculating sums.
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Produce a summarized DataFrame of financial transactions for reporting or analysis.
# MAGIC
# MAGIC **Input datasets:**  
# MAGIC - `base_df`
# MAGIC - `arlines_df`
# MAGIC - `oas_company_df`
# MAGIC - `oas_el2_element_df`
# MAGIC - `oas_el3_element_df`  
# MAGIC
# MAGIC **Output dataset:**  
# MAGIC - `final_agg_df` (Spark DataFrame with grouped and aggregated financial metrics)

# COMMAND ----------

# Join base with ARLines, company, EL2 element, and EL3 customer metadata
# Apply business filters for el2, ref2, el4, doccode, and docdate
# Group by key fields and aggregate valuehome and valuedoc with decimal point adjustment
final_agg_df = (
    base_df.alias("b")
    .join(
        arlines_df.alias("a"),
        (F.col("a.cmpcode") == F.col("b.cmpcode")) &
        (F.col("a.doccode") == F.col("b.doccode")) &
        (F.col("a.docnum") == F.col("b.docnum")),
        "inner"
    )
    .join(
        oas_company_df.alias("C"),
        F.col("C.code") == F.col("b.cmpcode"),
        "inner"
    )
    .join(
        oas_el2_element_df.alias("EL2"),
        (F.col("EL2.el2_cmpcode") == F.col("b.cmpcode")) &
        (F.col("EL2.el2_code") == F.col("b.el2")) &
        (F.col("EL2.el2_elmlevel") == F.lit(2)),
        "inner"
    )
    .join(
        oas_el3_element_df.alias("CUS"),
        (F.col("CUS.el3_cmpcode") == F.col("a.cmpcode")) &
        (F.col("CUS.el3_code") == F.col("a.el3")) &
        (F.col("CUS.el3_elmlevel") == F.lit(3)),
        "inner"
    )
    # Filter for relevant el2 codes and ranges
    .filter(
        (F.col("b.el2").like("4%")) |
        ((F.col("b.el2") >= "23160") & (F.col("b.el2") <= "23180")) |
        (F.col("b.cmpcode").isin("BAS", "WAR") & (F.col("b.el2") == "24300"))
    )
    # Exclude MAI recharges
    .filter(~((F.col("b.cmpcode") == "MAI") & (F.col("b.ref2").like("%Recharges%"))))
    # Exclude specific companies with el4 starting with X
    .filter(~(F.col("b.cmpcode").isin("ALLENTOWN", "MP", "INDY", "CLINTRAK") & F.col("b.el4").like("X%")))
    # Exclude unwanted doccodes
    .filter(~F.col("b.doccode").isin(
        "DISPERSE", "MATCHING", "PCANCEL", "PINV", "PINVDBT", "RECEIPTS", "Y/E-PROC-BS", "CPAY",
        "CREC", "OBAL", "PCRN", "REVAL", "REVALR", "YE-PROC-BS", "PINV_XL", "PINVDBT_XL",
        "PINV2", "PINV1", "ACCLTBI", "ACCLTBIREV", "ACCREV", "ACCRUAL", "CORRECTIVE",
        "PCRNIC", "PINVIC", "RECLASS", "REVACC", "REVERSAL", "JGEN", "JGENREV", "JREVGEN"
    ))
    # Restrict to docdate >= 2019-01-01
    .filter(F.col("b.docdate") >= F.lit("2019-01-01"))
    # Group by reporting fields
    .groupBy(
        "b.cmpcode",
        "b.global_identifier",
        "b.statpay",
        "b.docdate",
        "b.curdoc",
        "b.el3",
        "a.el3",
        "b.invoice_number",
        "b.po_value",
        "b.protocol_value",
        "b.docnum",
        "b.doccode",
        "b.el2",
        "b.el4",
        "C.homecur",
        "EL2.el2_name",
        "CUS.el3_name"
    )
    # Aggregate valuehome and valuedoc, adjusting for decimal points
    .agg(
        F.sum(
            F.when(F.col("b.valuehome_dp") == 0, F.col("b.valuehome"))
            .otherwise(F.col("b.valuehome") / F.pow(F.lit(10), F.col("b.valuehome_dp") - 2))
        ).alias("valuehome"),
        F.sum(
            F.when(F.col("b.valuedoc_dp") == 0, F.col("b.valuedoc"))
            .otherwise(F.col("b.valuedoc") / F.pow(F.lit(10), F.col("b.valuedoc_dp") - 2))
        ).alias("valuedoc")
    )
    # Select output columns for reporting
    .select(
        F.col("b.cmpcode"),
        F.col("b.global_identifier"),
        F.col("b.statpay"),
        F.col("b.docdate"),
        F.col("b.curdoc"),
        F.col("b.el3"),
        F.col("a.el3").alias("customer_number"),
        F.col("b.invoice_number"),
        F.col("b.po_value"),
        F.col("b.protocol_value"),
        F.col("b.docnum"),
        F.col("b.doccode"),
        F.col("b.el2"),
        F.col("b.el4"),
        F.col("C.homecur").alias("valuehomecurrency"),
        F.col("EL2.el2_name"),
        F.col("CUS.el3_name").alias("customer_name"),
        F.col("valuehome"),
        F.col("valuedoc")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### final_df
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Joins aggregated invoice/job data (`agg`) with job default values (`job_defaults`) on the `cmpcode` column.  
# MAGIC Maps `cmpcode` values to a `SiteID` using conditional logic.  
# MAGIC Selects and renames relevant columns for the final output, including coalescing `el4` with a default if empty.
# MAGIC
# MAGIC **Input datasets:**  
# MAGIC - `agg`: Aggregated invoice/job data (Spark DataFrame)  
# MAGIC - `job_defaults`: Default job values (Spark DataFrame)  
# MAGIC
# MAGIC **Output dataset:**  
# MAGIC - `final_df`: Transformed and joined DataFrame with selected and renamed columns, ready for downstream use.

# COMMAND ----------

# Final SELECT
final_df = (
    final_agg_df.alias("agg")
    .join(jobdefaults_df.alias("jd"), F.col("jd.cmpcode") == F.col("agg.cmpcode"), "left")
    .select(
        F.when(F.col("agg.cmpcode") == "ALLENTOWN", "0")
         .when(F.col("agg.cmpcode") == "BAS", "1")
         .when(F.col("agg.cmpcode") == "MAI", "2")
         .when(F.col("agg.cmpcode") == "MP", "4")
         .when(F.col("agg.cmpcode") == "WAR", "13")
         .when(F.col("agg.cmpcode") == "CLINTRAK", "99")
         .when(F.col("agg.cmpcode") == "INDY", "20")
         .when(F.col("agg.cmpcode") == "KOREA", "33")
         .when(F.col("agg.cmpcode") == "JAPAN", "15")
         .when(F.col("agg.cmpcode") == "SINGAPORE", "3")
         .when(F.col("agg.cmpcode") == "CHINA", "7")
         .when(F.col("agg.cmpcode") == "SUZHOU", "31")
         .when(F.col("agg.cmpcode") == "HEG", "38")
         .otherwise("Unknown")
         .alias("SiteID"),
        F.col("agg.cmpcode"),
        F.col("agg.global_identifier").alias("Global_Identifier"),
        F.col("agg.statpay"),
        F.col("agg.docdate").alias("InvoiceDate"),
        F.col("agg.valuehome"),
        F.col("agg.valuehomecurrency").alias("ValueHomeCurrency"),
        F.col("agg.valuedoc"),
        F.col("agg.curdoc"),
        F.col("agg.customer_number").alias("CustomerNumber"),
        F.col("agg.customer_name").alias("CustomerName"),
        F.col("agg.el3"),
        F.col("agg.invoice_number").alias("InvoiceNumber"),
        F.col("agg.po_value").alias("PO"),
        F.col("agg.protocol_value").alias("Protocol"),
        F.coalesce(F.when(F.col("agg.el4") != "", F.col("agg.el4")), F.col("jd.default_el4")).alias("JobNumber"),
        F.col("agg.docnum"),
        F.col("agg.doccode"),
        F.col("agg.el2"),
        F.col("agg.el2_name").alias("name")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final Code Validation

# COMMAND ----------

# display(final_df)

# COMMAND ----------

#Validation Checks: 1.schema 2.data 3.duplicates 4.count 5.data in s3
#final_df.printSchema()
#display(final_df)

#print(final_df.count()) 

# #Find duplicates
# final_df.createOrReplaceTempView("final_df_view")
# columns = final_df.columns
# columns_str = ", ".join([f"`{col}`" for col in columns])
# query = f"""
#     SELECT {columns_str}, COUNT(*) as count
#     FROM final_df_view
#     GROUP BY {columns_str}
#     HAVING COUNT(*) > 1
# """
# duplicate_records_df = spark.sql(query)
# display(duplicate_records_df)

# COMMAND ----------

# MAGIC %md
# MAGIC =================5426===============

# COMMAND ----------

# MAGIC %md
# MAGIC ### Documentation Overview
# MAGIC This code processes financial invoice data by transforming, formatting, and enriching it with additional attributes and reference mappings.
# MAGIC
# MAGIC #### Input Datasets
# MAGIC - `final_df`: Spark DataFrame containing cert AQE MDP.
# MAGIC - `mapEntity_df`: Spark DataFrame mapping company codes to entity codes.
# MAGIC - `zInvoiceCode_df`: Spark DataFrame mapping facility GL codes to CPQ categories.
# MAGIC
# MAGIC #### Steps
# MAGIC 1. **Select relevant columns** from `final_df` for `base_data`.
# MAGIC 2. **Cast** `InvoiceDate` to date, `valuedoc` and `valuehome` to double in `typed_data`.
# MAGIC 3. **Format** `valuedoc` and `valuehome` as currency strings in `formatted_data`.
# MAGIC 4. **Create custom key** `key_global_identifier` by concatenating `SiteID`, `InvoiceNumber`, and `el3` in `custom_keys`.
# MAGIC 5. **Generate** `Pass_Through_Flag` based on `el3` and `cmpcode` logic in `pass_through_flags`.
# MAGIC 6. **Classify** `Pass_Through_Flag2` for pass-through transactions in `pass_through_flags2`.
# MAGIC 7. **Join** with `mapEntity_df` to add `MAP_Entity_CODA` in `joined_entity`.
# MAGIC 8. **Create** `Site_GL_CODE` by uppercasing the concatenation of `cmpcode` and `el3` in `site_gl_code`.
# MAGIC 9. **Join** with `zInvoiceCode_df` to enrich with `CPQ Category` and `SubCategory` in `joined_cpq`.
# MAGIC 10. **Add** a `Source` column with value `"CODA"` in `output_df`.
# MAGIC
# MAGIC #### Output
# MAGIC - `output_df`: Spark DataFrame containing transformed invoice data with enriched attributes for downstream analysis or reporting.

# COMMAND ----------

# DBTITLE 1,5426-joined dataset: MDP AQE, MapEntity, zInvoiceCodes
# Select relevant columns from final_df for base_data-final_df is cert AQE MDP dataset
base_data = final_df.select(
    "SiteID", "cmpcode", "Global_Identifier", "statpay", "InvoiceDate", "valuehome",
    "ValueHomeCurrency", "valuedoc", "curdoc", "CustomerNumber", "CustomerName",
    "el3", "InvoiceNumber", "PO", "Protocol", "JobNumber", "docnum", "doccode", "el2", "name"
)

# Cast InvoiceDate to date, valuedoc and valuehome to double
typed_data = base_data.withColumn(
    "InvoiceDate", F.left(F.col("InvoiceDate").cast("string"), F.lit(10)).cast("date")
).withColumn(
    "valuedoc", F.col("valuedoc").cast("double")
).withColumn(
    "valuehome", F.col("valuehome").cast("double")
)

# Format valuedoc and valuehome as currency strings
formatted_data = typed_data.withColumn(
    "valuedoc_formatted", F.format_string("$%,.2f", F.col("valuedoc"))
).withColumn(
    "valuehome_formatted", F.format_string("$%,.2f", F.col("valuehome"))
)

# Create custom key by concatenating SiteID, InvoiceNumber, and el3
custom_keys = formatted_data.withColumn(
    "Key_global_identifier", F.concat(F.col("SiteID"), F.col("InvoiceNumber"), F.col("el3"))
)

# Derive Pass_Through_Flag based on cmpcode and el3
pass_through_flags = custom_keys.withColumn(
    "Pass_Through_Flag",
    F.when(F.col("el3").isNull(), F.lit("Other"))
    .when(F.col("cmpcode") == "ALLENTOWN", F.left(F.col("el3"), F.lit(1)))
    .when(F.col("cmpcode") == "MAI", F.concat(F.col("cmpcode"), F.col("el3")))
    .when(F.col("cmpcode") == "BAS", F.concat(F.col("cmpcode"), F.col("el3")))
    .otherwise(F.lit("Other"))
)

# Classify Pass_Through_Flag2 based on Pass_Through_Flag patterns
pass_through_flags2 = pass_through_flags.withColumn(
    "Pass_Through_Flag2",
    F.when(F.col("Pass_Through_Flag").like("%X%"), F.lit("PT"))
    .when(F.left(F.col("Pass_Through_Flag"), F.lit(3)) == "BAS", F.lit("PT"))
    .when(F.col("Pass_Through_Flag") == "BASD4199", F.lit("PT"))
    .when(F.col("Pass_Through_Flag") == "MAID4400", F.lit("PT"))
    .when(F.col("Pass_Through_Flag").like("%x%"), F.lit("PT"))
    .otherwise(F.lit("Not classified as PT"))
)

# Join with mapEntity_df to add MAP_Entity_CODA
joined_entity = pass_through_flags2.join(
    mapEntity_df.select(F.col("CODA").alias("MAP_Entity_CODA")),
    pass_through_flags2["cmpcode"] == F.col("MAP_Entity_CODA"),
    "left"
)

# Create Site_GL_CODE by concatenating cmpcode and el3 in uppercase
site_gl_code = joined_entity.withColumn(
    "Site_GL_CODE", F.upper(F.concat(F.col("cmpcode"), F.col("el3")))
)

# Join with zInvoiceCode_df to add CPQ Category and SubCategory
joined_cpq = site_gl_code.join(
    zInvoiceCode_df.select(
        F.col("Facility|GLCode-CPQREF"),
        F.col("AI Enhanced - CPQ Category").alias("CPQ Category"),
        F.col("AI Enhanced - CPQ SubCategory").alias("CPQ SubCategory")
    ),
    site_gl_code["Site_GL_CODE"] == zInvoiceCode_df["Facility|GLCode-CPQREF"],
    "left"
)

# Add Source column with value "CODA"
output_df = joined_cpq.withColumn("Source", F.lit("CODA"))

# COMMAND ----------

# output_df = output_df.toDF(
#     *[
#         col_name.replace(' ', '_')
#         .replace(',', '_')
#         .replace(';', '_')
#         .replace('{', '_')
#         .replace('}', '_')
#         .replace('(', '_')
#         .replace(')', '_')
#         .replace('\n', '_')
#         .replace('\t', '_')
#         .replace('=', '_')
#         for col_name in output_df.columns
#     ]
# )

# output_df.write \
#     .mode("overwrite") \
#     .format("delta") \
#     .option("mergeSchema", "true") \
#     .save("s3://tfsdl-corp-fdt/test/psg/ctd/cert/aqe_ref/psg_ctd_cert_aqe_mdp_5426")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Code Validation

# COMMAND ----------

#Validation Checks: 1.schema 2.data 3.duplicates 4.count 5.data in s3
#output_df.printSchema()
#display(output_df)

#print(output_df.count()) 

# #Find duplicates
# output_df.createOrReplaceTempView("final_df_view")
# columns = final_df.columns
# columns_str = ", ".join([f"`{col}`" for col in columns])
# query = f"""
#     SELECT {columns_str}, COUNT(*) as count
#     FROM final_df_view
#     GROUP BY {columns_str}
#     HAVING COUNT(*) > 1
# """
# duplicate_records_df = spark.sql(query)
# display(duplicate_records_df)
