# Databricks notebook source
# MAGIC %md
# MAGIC #### Overview
# MAGIC
# MAGIC This notebook processes invoice and delivery site data from multiple tables, applies business logic to derive key fields, and corrects reference values using a correction table. The final output is a table of invoices with mapped delivery sites, corrected references, and filtered for valid invoice numbers.
# MAGIC
# MAGIC **Input tables:**
# MAGIC - `oas_docline`: Invoice line details
# MAGIC - `oas_dochead`: Invoice header details
# MAGIC - `oas_company`: Company reference data
# MAGIC - `oas_el3_element`: Element reference data
# MAGIC - `coda_ref_correction`: Reference correction mappings

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Libraries

# COMMAND ----------

# DBTITLE 1,Importing Libraries
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

# DBTITLE 1,Utility Functions
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

# S3 path for OAS document line Delta table
oas_docline_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_docline"

# S3 path for OAS document head Delta table
oas_dochead_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_dochead"

# S3 path for OAS company Delta table
oas_company_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_company"

# S3 path for OAS EL3 element Delta table
oas_el3_element = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el3_element"

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
file_name_codaCorrection = "CODA-Reference Correction.xlsx"
file_name_mapEntity="MAP-Entity.xlsx"
file_name_mapIC_Entity="MAP-IC_Entity.xlsx"


# COMMAND ----------

# MAGIC %md
# MAGIC ####Input Datasets

# COMMAND ----------

# DBTITLE 1,Creating Input Datasets
# Load OAS document line Delta table from S3 and select relevant columns
oas_docline_df = (
    spark.read.format("delta")
    .load(oas_docline_str)
    .select(
        "cmpcode",
        "doccode",
        "docnum",
        "statpay",
        "valuedoc",
        "valuehome",
        "valuedoc_dp",
        "valuehome_dp",
        "el2",
        "el3",
        "el4",
        "ref1",
        "ref2",
        "ref3",
        "ref4",
        "ref5"
    )
)

# Load OAS document head Delta table from S3 and select relevant columns
oas_dochead_df = spark.read.format("delta").load(oas_dochead_str).select("cmpcode","doccode","docdate","docnum")

# Load OAS company Delta table from S3 and select relevant columns
oas_company_df = spark.read.format("delta").load(oas_company_str).select("code")

# Load OAS EL3 element Delta table from S3 and select relevant columns
oas_el3_element_df = spark.read.format("delta").load(oas_el3_element).select("el3_name","el3_code","el3_cmpcode","el3_elmlevel")

# Download SharePoint Excel file and load as pandas DataFrame
coda_ref_correction_df = get_sharepoint_excel_df(
            weburl + '/', file_name_codaCorrection, client_id, client_secret, driver_id,
            skip_num_rows=0, sheet_prefix="Sheet1")
# Convert pandas DataFrame to Spark DataFrame for coda_ref_correction_df
coda_ref_correction_df = spark.createDataFrame(coda_ref_correction_df)       

# Download SharePoint Excel file and load as pandas DataFrame
map_entity_df = get_sharepoint_excel_df(
            weburl + '/', file_name_mapEntity, client_id, client_secret, driver_id,
            skip_num_rows=0, sheet_prefix="Sheet1")
# Convert pandas DataFrame to Spark DataFrame for coda_ref_correction_df
map_entity_df = spark.createDataFrame(map_entity_df) 

# Download SharePoint Excel file and load as pandas DataFrame
map_ICentity_df = get_sharepoint_excel_df(
            weburl + '/', file_name_mapIC_Entity, client_id, client_secret, driver_id,
            skip_num_rows=0, sheet_prefix="Sheet1")
# Convert pandas DataFrame to Spark DataFrame for coda_ref_correction_df
map_ICentity_df = spark.createDataFrame(map_ICentity_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating `Aggregated Dataset`
# MAGIC
# MAGIC - **Source Dataset:**  
# MAGIC   - `oas_docline_df`
# MAGIC
# MAGIC - **Output Dataset:**  
# MAGIC   - `aggregated_df`
# MAGIC
# MAGIC - **Logic:** Aggregates document line records where `el2` starts with '021'.
# MAGIC - **Key Calculations:**
# MAGIC   - Sums `valuedoc` and `valuehome` after scaling by their decimal precision fields (`valuedoc_dp`, `valuehome_dp`).
# MAGIC   - Uses `CASE` to treat null or zero decimal precision as zero value.
# MAGIC   - Picks the maximum value for reference fields (`el2`, `el3`, `ref1`-`ref5`) per group.
# MAGIC - **Grouping:** By `cmpcode`, `doccode`, `docnum`, `statpay`.

# COMMAND ----------

# DBTITLE 1,Output Dataset: aggregated_df
# Aggregate document line records where el2 starts with '021'
aggregated_df = (
    oas_docline_df
      .filter(F.col("el2").startswith("021"))  # Filter rows where el2 starts with '021'
      .select(
          "cmpcode", "doccode", "docnum", "statpay",
          "el2", "el3", "ref1", "ref2", "ref3", "ref4", "ref5",
          # Calculate valuedoc using decimal precision; treat null/zero as 0
          (F.when(F.col("valuedoc_dp").isNull() | (F.col("valuedoc_dp") == 0), F.lit(0))
           .otherwise(F.col("valuedoc") / F.pow(10, F.col("valuedoc_dp") - 2))
          ).alias("valuedoc_calc"),
          # Calculate valuehome using decimal precision; treat null/zero as 0
          (F.when(F.col("valuehome_dp").isNull() | (F.col("valuehome_dp") == 0), F.lit(0))
           .otherwise(F.col("valuehome") / F.pow(10, F.col("valuehome_dp") - 2))
          ).alias("valuehome_calc")
      )
      .groupBy("cmpcode", "doccode", "docnum", "statpay")  # Group by key fields
      .agg(
          F.sum("valuedoc_calc").alias("valuedoc"),         # Sum valuedoc per group
          F.sum("valuehome_calc").alias("valuehome"),       # Sum valuehome per group
          F.max("el2").alias("el2"),                        # Max el2 per group
          F.max("el3").alias("el3"),                        # Max el3 per group
          F.max("ref1").alias("ref1"),                      # Max ref1 per group
          F.max("ref2").alias("ref2"),                      # Max ref2 per group
          F.max("ref3").alias("ref3"),                      # Max ref3 per group
          F.max("ref4").alias("ref4"),                      # Max ref4 per group
          F.max("ref5").alias("ref5")                       # Max ref5 per group
      )
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating Dataset `Aggregated Dataset2` - Extracts additional attributes
# MAGIC
# MAGIC - **Source Dataset:**  
# MAGIC   - `oas_docline_df`
# MAGIC
# MAGIC - **Output Dataset:**  
# MAGIC   - `aggregated_df2`
# MAGIC
# MAGIC - **Logic:** Aggregates document line records where `el2` does **not** start with '021' and `el4` is not empty.
# MAGIC - **Key Calculation:** Picks the maximum `el4` value per invoice.
# MAGIC - **Grouping:** By `cmpcode`, `doccode`, `docnum`.

# COMMAND ----------

# DBTITLE 1,OutputDataset: aggregated_df2
# Aggregate document line records where el2 does NOT start with '021' and el4 is not empty
aggregated_df2 = (
    oas_docline_df
      .filter((~F.col("el2").like("021%")) & (F.col("el4") != ""))  # Filter rows: el2 not '021%', el4 not empty
      .groupBy("cmpcode", "doccode", "docnum")                      # Group by invoice keys
      .agg(F.max("el4").alias("el4"))                               # Get max el4 per group
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating `Final Aggregated Dataset - Enriching and Mapping Data` 
# MAGIC
# MAGIC - **Source Datasets:**  
# MAGIC   - `oas_dochead_df`
# MAGIC   - `aggregated_df`
# MAGIC   - `aggregated_df2`
# MAGIC   - `oas_company_df`
# MAGIC   - `oas_el3_element_df`
# MAGIC
# MAGIC - **Output Dataset:**  
# MAGIC   - `final_aggregated_df`
# MAGIC
# MAGIC - **Logic:** 
# MAGIC   - Maps company codes to business site IDs.
# MAGIC   - Determines invoice number, type, service delivery site, and other business fields using `CASE` logic.
# MAGIC
# MAGIC - **Purpose:** Produces a business-ready, enriched final dataset with all key attributes and mappings applied.
# MAGIC
# MAGIC #### Filtering and Finalizing the Dataset
# MAGIC
# MAGIC - Filters out records with payment status 665, certain document codes.

# COMMAND ----------

# DBTITLE 1,Output Dataset: final_aggregated_df
final_aggregated_df = (
    oas_dochead_df.alias("DH")
    # Join with AggregatedDL (aggregated_df) on cmpcode, doccode, docnum
    .join(
        aggregated_df.alias("DL"),
        (F.trim(F.col("DL.cmpcode")) == F.trim(F.col("DH.cmpcode"))) &
        (F.trim(F.col("DL.doccode")) == F.trim(F.col("DH.doccode"))) &
        (F.trim(F.col("DL.docnum")) == F.trim(F.col("DH.docnum"))),
        "left"
    )
    # Join with AggregatedDL2 (aggregated_df2) for el4 extraction
    .join(
        aggregated_df2.alias("DL2"),
        (F.trim(F.col("DL2.cmpcode")) == F.trim(F.col("DL.cmpcode"))) &
        (F.trim(F.col("DL2.doccode")) == F.trim(F.col("DL.doccode"))) &
        (F.trim(F.col("DL2.docnum")) == F.trim(F.col("DL.docnum"))),
        "left"
    )
    # Join with company table for company code validation
    .join(
        oas_company_df.alias("COMP"),
        F.col("COMP.code") == F.col("DL.cmpcode"),
        "left"
    )
    # Join with EL3 element table for customer name enrichment
    .join(
        oas_el3_element_df.alias("ELE"),
        (F.col("ELE.el3_cmpcode") == F.col("DL.cmpcode")) &
        (F.col("ELE.el3_code") == F.col("DL.el3")) &
        (F.col("ELE.el3_elmlevel") == F.lit(3)),
        "left"
    )
    # Filter for valid company codes, payment status, date, and exclude certain doccodes
    .filter(
        F.col("DH.cmpcode").isin(
            'ALLENTOWN', 'MAI', 'MP', 'BAS', 'WAR', 'CLINTRAK', 'SINGAPORE',
            'INDY', 'HEG', 'SUZHOU', 'CHINA', 'JAPAN', 'KOREA'
        ) &
        (F.col("DL.statpay") != 665) &
        (F.col("DH.docdate") >= F.lit("2019-01-01")) &
        (~F.col("DH.doccode").isin(
            'DISPERSE', 'MATCHING', 'PCANCEL', 'PINV', 'PINVDBT', 'RECEIPTS',
            'Y/E-PROC-BS', 'CPAY', 'CREC', 'OBAL', 'PCRN', 'REVAL', 'REVALR',
            'YE-PROC-BS', 'PINV_XL', 'PINVDBT_XL', 'PINV2', 'PINV1', 'ACCLTBI',
            'ACCLTBIREV', 'ACCREV', 'ACCRUAL', 'CORRECTIVE', 'PCRNIC', 'PINVIC',
            'RECLASS', 'REVACC', 'REVERSAL', 'JGEN', 'JGENREV', 'JREVGEN'
        ))
    )
    .select(
        # Map company code to SiteID
        F.when(F.col("DH.cmpcode") == "ALLENTOWN", F.lit("0"))
         .when(F.col("DH.cmpcode") == "BAS", F.lit("1"))
         .when(F.col("DH.cmpcode") == "MAI", F.lit("2"))
         .when(F.col("DH.cmpcode") == "MP", F.lit("4"))
         .when(F.col("DH.cmpcode") == "WAR", F.lit("13"))
         .when(F.col("DH.cmpcode") == "CLINTRAK", F.lit("99"))
         .when(F.col("DH.cmpcode") == "INDY", F.lit("20"))
         .when(F.col("DH.cmpcode") == "KOREA", F.lit("33"))
         .when(F.col("DH.cmpcode") == "JAPAN", F.lit("15"))
         .when(F.col("DH.cmpcode") == "SINGAPORE", F.lit("3"))
         .when(F.col("DH.cmpcode") == "CHINA", F.lit("7"))
         .when(F.col("DH.cmpcode") == "SUZHOU", F.lit("31"))
         .when(F.col("DH.cmpcode") == "HEG", F.lit("38"))
         .otherwise(F.lit("Unknown"))
         .alias("SiteID"),
        F.col("DH.cmpcode"),
        F.col("DH.doccode"),
        F.col("DH.docdate").alias("InvoiceDate"),
        # Determine InvoiceNumber based on company code
        F.when(F.col("DH.cmpcode").isin('ALLENTOWN', 'MAI', 'CHINA', 'SINGAPORE', 'CLINTRAK', 'MP'), F.col("DL.ref1"))
         .when(F.col("DH.cmpcode").isin('JAPAN', 'SUZHOU'), F.col("DL.ref5"))
         .otherwise(F.ltrim(F.col("DH.docnum"))).alias("InvoiceNumber"),
        # Determine DocType (Credit Note or Invoice)
        F.when(F.col("DH.doccode").like("%CR%"), F.lit("Credit Note"))
         .otherwise(F.lit("Invoice")).alias("DocType"),
        # ServiceDeliverySite logic based on company and reference fields
        F.upper(
            F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.substring(F.col("DL.ref5"), 1, 6))
             .when((F.col("DH.cmpcode") == "BAS") & (~F.col("DL.ref5").like("PT%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.ref5").like("PT%")), F.substring(F.col("DL.ref5"), 1, 6))
             .when((F.col("DH.cmpcode") == "WAR") & (~F.col("DL.ref5").like("PT%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
             .when((F.col("DH.cmpcode") == "ALLENTOWN") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "MP") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
             .when((F.col("DH.cmpcode") == "MP") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
             .when((F.col("DH.cmpcode") == "CLINTRAK") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.ref3").like("PT%")), F.substring(F.col("DL.ref3"), 1, 6))
             .when((F.col("DH.cmpcode") == "MAI") & (~F.col("DL.ref3").like("PT%")), F.col("DH.cmpcode"))
             .otherwise("Unknown")
        ).alias("ServiceDeliverySite"),
        # ServiceDeliverySiteInvoiceNumber logic
        F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.expr("substring(DL.ref5, 8, length(DL.ref5))"))
         .when((F.col("DH.cmpcode") == "BAS") & (~F.col("DL.ref5").like("PT%")), F.ltrim(F.col("DH.docnum")))
         .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.ref5").like("PT%")), F.expr("substring(DL.ref5, 8, length(DL.ref5))"))
         .when((F.col("DH.cmpcode") == "WAR") & (~F.col("DL.ref5").like("PT%")), F.ltrim(F.col("DH.docnum")))
         .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL2.el4").like("X%")), F.col("DL.ref5"))
         .when((F.col("DH.cmpcode") == "ALLENTOWN") & (~F.col("DL2.el4").like("X%")), F.col("DL.ref1"))
         .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL2.el4").like("X%")), F.col("DL.ref5"))
         .when((F.col("DH.cmpcode") == "CLINTRAK") & (~F.col("DL2.el4").like("X%")), F.col("DL.ref1"))
         .when((F.col("DH.cmpcode") == "MP") & (F.col("DL2.el4").like("X%")), F.col("DL.ref5"))
         .when((F.col("DH.cmpcode") == "MP") & (~F.col("DL2.el4").like("X%")), F.col("DL.ref1"))
         .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.ref3").like("PT%")), F.expr("substring(DL.ref3, 8, length(DL.ref3))"))
         .when((F.col("DH.cmpcode") == "MAI") & (~F.col("DL.ref3").like("PT%")), F.col("DL.ref1"))
         .otherwise("Unknown")
         .alias("ServiceDeliverySiteInvoiceNumber"),
        # InvoiceType logic
        F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.el3").like("ICOM%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.el3").like("ICOM%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.ref5").like("PT%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL.el3").like("X%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL2.el4").like("X%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL.el3").like("X%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL2.el4").like("X%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "MP") & (F.col("DL.el3").like("X%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "MP") & (F.col("DL2.el4").like("X%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.el3").like("IF%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.ref3").like("PT%")), F.lit("3PD-PT"))
         .otherwise(F.lit("3PD-OWN"))
         .alias("InvoiceType"),
        # ICEntityMapKey logic
        F.upper(
            F.concat(
                F.col("DH.cmpcode"),
                F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.substring(F.col("DL.ref5"), 1, 6))
                 .when((F.col("DH.cmpcode") == "BAS") & (~F.col("DL.ref5").like("PT%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.ref5").like("PT%")), F.substring(F.col("DL.ref5"), 1, 6))
                 .when((F.col("DH.cmpcode") == "WAR") & (~F.col("DL.ref5").like("PT%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
                 .when((F.col("DH.cmpcode") == "ALLENTOWN") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "MP") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
                 .when((F.col("DH.cmpcode") == "MP") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
                 .when((F.col("DH.cmpcode") == "CLINTRAK") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.ref3").like("PT%")), F.substring(F.col("DL.ref3"), 1, 6))
                 .when((F.col("DH.cmpcode") == "MAI") & (~F.col("DL.ref3").like("PT%")), F.col("DH.cmpcode"))
                 .otherwise("Unknown")
            )
        ).alias("ICEntityMapKey"),
        # _keyClientID logic
        F.concat(
            F.when(F.col("DH.cmpcode") == "ALLENTOWN", F.lit("0"))
             .when(F.col("DH.cmpcode") == "BAS", F.lit("1"))
             .when(F.col("DH.cmpcode") == "MAI", F.lit("2"))
             .when(F.col("DH.cmpcode") == "MP", F.lit("4"))
             .when(F.col("DH.cmpcode") == "WAR", F.lit("13"))
             .when(F.col("DH.cmpcode") == "CLINTRAK", F.lit("99"))
             .when(F.col("DH.cmpcode") == "INDY", F.lit("20"))
             .when(F.col("DH.cmpcode") == "KOREA", F.lit("33"))
             .when(F.col("DH.cmpcode") == "JAPAN", F.lit("15"))
             .when(F.col("DH.cmpcode") == "SINGAPORE", F.lit("3"))
             .when(F.col("DH.cmpcode") == "CHINA", F.lit("7"))
             .when(F.col("DH.cmpcode") == "SUZHOU", F.lit("31"))
             .when(F.col("DH.cmpcode") == "HEG", F.lit("38"))
             .otherwise(F.lit("Unknown")),
            F.lit("-"),
            F.col("DL.el3")
        ).alias("_keyClientID"),
        F.col("DL.el3").alias("CustomerNumber"),
        F.col("ELE.el3_name").alias("CustomerName"),
        # PO logic
        F.when(F.col("DH.cmpcode") == "CHINA", F.col("DL.ref2"))
         .when(F.col("DH.cmpcode") == "WAR", F.col("DL.ref3"))
         .otherwise(F.col("DL.ref4")).alias("PO"),
        # Protocol logic
        F.when(F.col("DH.cmpcode").isin("JAPAN", "SUZHOU"), F.col("DL.ref3"))
         .otherwise(F.col("DL.ref2")).alias("Protocol"),
        # JobNumber logic
        F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.col("DL.ref3"))
         .otherwise(F.coalesce(F.col("DL2.el4"), F.lit(""))).alias("JobNumber"),
        F.col("DL.statpay"),
        F.col("DL.valuehome").cast("double").alias("valuehome"),
        F.col("DL.valuedoc").cast("double").alias("valuedoc")
    )
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adds two new columns to `final_aggregated_df`:
# MAGIC
# MAGIC - **Source Dataset:**  
# MAGIC   - `final_aggregated_df`
# MAGIC
# MAGIC - **Output Dataset:**  
# MAGIC   - `final_aggregated_df2`
# MAGIC
# MAGIC - **InterimKey_SVCDVSite:** Concatenates `cmpcode`, `doccode`, trimmed `InvoiceNumber`, and `"ServiceDeliverySite"` (hyphen-separated).
# MAGIC - **InterimKey_SVCDVSiteIN:** Concatenates `cmpcode`, `doccode`, trimmed `InvoiceNumber`, and `"ServiceDeliverySiteInvoiceNumber"` (hyphen-separated).

# COMMAND ----------

# DBTITLE 1,Output Dataset: final_aggregated_df2
# create final_aggregated_df2 dataset
final_aggregated_df2 = (
    final_aggregated_df
    # Create InterimKey_SVCDVSite: cmpcode-doccode-trimmed InvoiceNumber-ServiceDeliverySite (hyphen-separated)
    .withColumn(
        "InterimKey_SVCDVSite",
        F.concat_ws(
            "-", 
            F.col("cmpcode"),
            F.col("doccode"),
            F.trim(F.col("InvoiceNumber")),
            F.lit("ServiceDeliverySite")
        )
    )
    # Create InterimKey_SVCDVSiteIN: cmpcode-doccode-trimmed InvoiceNumber-ServiceDeliverySiteInvoiceNumber (hyphen-separated)
    .withColumn(
        "InterimKey_SVCDVSiteIN",
        F.concat_ws(
            "-", 
            F.col("cmpcode"),
            F.col("doccode"),
            F.trim(F.col("InvoiceNumber")),
            F.lit("ServiceDeliverySiteInvoiceNumber")
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create CODA Reference Correction Dataset
# MAGIC
# MAGIC - **Source Dataset:**  
# MAGIC   - `coda_ref_correction_df` (loaded from SharePoint)
# MAGIC
# MAGIC - **Output Dataset:**  
# MAGIC   - `coda_referenceCorrection_df`
# MAGIC
# MAGIC - **Purpose:** Creates a new DataFrame `coda_referenceCorrection_df` from `coda_ref_correction_df` (previously loaded this from sharepoint).
# MAGIC - **Columns To derive or Select:**
# MAGIC   - `InterimKey`: Concatenation of `Instance`, `DocCode`, `Title`, and `Field` (hyphen-separated).
# MAGIC   - `FieldValue`: The original field value.

# COMMAND ----------

# DBTITLE 1,Output Dataset: coda_referenceCorrection_df
# CODA Reference Correction
# Create a DataFrame with InterimKey (concatenation of Instance, DocCode, Title, Field) and FieldValue
coda_referenceCorrection_df = (
    coda_ref_correction_df
    .select(
        F.concat_ws("-", "Instance", "DocCode", "Title", "Field").alias("InterimKey"),
        F.col("FieldValue")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join and Adding Columns
# MAGIC
# MAGIC - **Source Datasets:**  
# MAGIC   - `final_aggregated_df2`
# MAGIC   - `coda_referenceCorrection_df`
# MAGIC
# MAGIC - **Output Dataset:**  
# MAGIC   - `merged_df`
# MAGIC
# MAGIC - **Join Logic:**  
# MAGIC   Join `final_aggregated_df2` with `coda_referenceCorrection_df` using a left join where either `InterimKey_SVCDVSite` or `InterimKey_SVCDVSiteIN` matches `InterimKey`.
# MAGIC
# MAGIC - **Correction Columns:**  
# MAGIC   Add two new columns:  
# MAGIC   - `Corrected_Originating_Invoice`: Set to `FieldValue` from `coda_referenceCorrection_df`.  
# MAGIC   - `Corrected_OriginatingSite`: Set to `FieldValue` from `coda_referenceCorrection_df`.

# COMMAND ----------

# DBTITLE 1,Output Dataset: merged_df
# Join final_aggregated_df2 with coda_referenceCorrection_df using a left join
# The join condition matches either InterimKey_SVCDVSite or InterimKey_SVCDVSiteIN to InterimKey
# Add two new columns: Corrected_Originating_Invoice and Corrected_OriginatingSite, both set to FieldValue from the correction table
merged_df = (
    final_aggregated_df2
    .join(
        coda_referenceCorrection_df,
        (F.col("InterimKey_SVCDVSite") == F.col("InterimKey")) | 
        (F.col("InterimKey_SVCDVSiteIN") == F.col("InterimKey")),
        "left"
    )
    .withColumn("Corrected_Originating_Invoice", F.col("FieldValue"))  # Corrected_Originating_Invoice
    .withColumn("Corrected_OriginatingSite", F.col("FieldValue"))      # Corrected_OriginatingSite
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Final Dataset: `result_df`
# MAGIC
# MAGIC - **Input Source:**  
# MAGIC   - `merged_df` (joined from `final_aggregated_df2` and `coda_referenceCorrection_df`)
# MAGIC
# MAGIC - **Output Dataset:**  
# MAGIC   - `result_df`
# MAGIC
# MAGIC 1. **New_ServiceDeliverySite**  
# MAGIC    - If `Corrected_OriginatingSite` is null, use `ServiceDeliverySite`; otherwise, use `Corrected_OriginatingSite`.
# MAGIC
# MAGIC 2. **New_ServiceDeliverySiteInvoiceNumber**  
# MAGIC    - If `Corrected_Originating_Invoice` is null and `ServiceDeliverySiteInvoiceNumber` is empty, use `InvoiceNumber`.
# MAGIC    - If `Corrected_Originating_Invoice` is null, use `ServiceDeliverySiteInvoiceNumber`.
# MAGIC    - Otherwise, use `Corrected_Originating_Invoice`.
# MAGIC
# MAGIC 3. **_InvoiceSite_DeliverySite**  
# MAGIC    - Concatenate `cmpcode` and `New_ServiceDeliverySite`.
# MAGIC
# MAGIC 4. **Filtering**  
# MAGIC    - Exclude rows where `New_ServiceDeliverySiteInvoiceNumber` (case-insensitive) contains "REIMB" or "olidated".

# COMMAND ----------

# DBTITLE 1,Output Dataset: result_df
# Create result_df with correction columns and filter logic
result_df = (
    merged_df
    # New_ServiceDeliverySite: Use Corrected_OriginatingSite if available, else ServiceDeliverySite
    .withColumn(
        "New_ServiceDeliverySite",
        F.when(F.col("Corrected_OriginatingSite").isNull(), F.col("ServiceDeliverySite"))
         .otherwise(F.col("Corrected_OriginatingSite"))
    )
    # New_ServiceDeliverySiteInvoiceNumber: Use correction if available, else fallback logic
    .withColumn(
        "New_ServiceDeliverySiteInvoiceNumber",
        F.when(
            F.col("Corrected_Originating_Invoice").isNull() & (F.col("ServiceDeliverySiteInvoiceNumber") == ""),
            F.col("InvoiceNumber")
        )
        .when(F.col("Corrected_Originating_Invoice").isNull(), F.col("ServiceDeliverySiteInvoiceNumber"))
        .otherwise(F.col("Corrected_Originating_Invoice"))
    )
    # _InvoiceSite_DeliverySite: Concatenate cmpcode and New_ServiceDeliverySite
    .withColumn(
        "_InvoiceSite_DeliverySite",
        F.concat(F.col("cmpcode"), F.col("New_ServiceDeliverySite"))
    )
    # Filter out rows where New_ServiceDeliverySiteInvoiceNumber contains 'REIMB' or 'olidated' (case-insensitive)
    .filter(
        ~F.upper(F.coalesce(F.col("New_ServiceDeliverySiteInvoiceNumber"), F.lit("[]"))).like("%REIMB%") &
        ~F.lower(F.coalesce(F.col("New_ServiceDeliverySiteInvoiceNumber"), F.lit("[]"))).like("%olidated%")
    )
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final Code Validation

# COMMAND ----------

#Validation Checks: 1.schema 2.data 3.duplicates 4.count 5.data in s3
#result_df.printSchema()
#display(result_df)

#print(result_df.count())  #1305270

# #Find duplicates
# result_df.createOrReplaceTempView("final_df_view")
# #final_df_dedup.createOrReplaceTempView("final_df_view")
# columns = result_df.columns
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
# MAGIC ###=================5425===============

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview
# MAGIC
# MAGIC This notebook processes invoice data by joining, transforming, and generating key identifiers for AR (Accounts Receivable) reporting. It integrates mapping tables to enrich invoice records, applies business logic for key generation, and standardizes site names.
# MAGIC
# MAGIC ### Purpose
# MAGIC
# MAGIC - Filter and clean invoice records.
# MAGIC - Enrich invoices with site and intercompany mapping data.
# MAGIC - Generate unique keys for AR and intercompany reconciliation.
# MAGIC - Standardize site names for reporting consistency.
# MAGIC
# MAGIC ### Input Datasets
# MAGIC
# MAGIC - `result_df`: Spark DataFrame containing invoice records (which is cert aqe query results derived from above).
# MAGIC - `map_entity_df`: Spark DataFrame mapping site codes to GPM site and cForia values.
# MAGIC - `map_ICentity_df`: Spark DataFrame mapping intercompany partner information.
# MAGIC
# MAGIC ### Output
# MAGIC
# MAGIC - `final_df`: Spark DataFrame with AR reporting columns, enriched and standardized for downstream analysis.

# COMMAND ----------

# Step 1: Filter rows on cert aqe dataset(its called result_df) with non-null and non-empty InvoiceNumber
filtered_rows1 = (
    result_df
    .filter(
        (F.col("InvoiceNumber").isNotNull()) & (F.col("InvoiceNumber") != "")
    )
)

# Step 2: Merge with MAP-Entity table on cmpcode = CODA
merged_map_entity = (
    filtered_rows1.join(
        map_entity_df,
        filtered_rows1["cmpcode"] == map_entity_df["CODA"],
        "left"
    )
    .select(
        filtered_rows1["*"],
        map_entity_df["gpm_site"],
        map_entity_df["cForia"]
    )
)

# Step 3: Merge with MAP-IC_Entity table on _InvoiceSite_DeliverySite = InvoiceCmp_OriginatingIC
# Rename columns with hyphens to underscores for compatibility
map_ICentity_df_fixed = (
    map_ICentity_df
    .withColumnRenamed("InvoiceCmp-OriginatingIC", "InvoiceCmp_OriginatingIC")
    .withColumnRenamed("IC_Partner-OMNI", "IC_Partner_OMNI")
    .withColumnRenamed("IC_Partner-CODA", "IC_Partner_CODA")
)

merged_map_ic_entity = (
    merged_map_entity.join(
        map_ICentity_df_fixed,
        merged_map_entity["_InvoiceSite_DeliverySite"] == map_ICentity_df_fixed["InvoiceCmp_OriginatingIC"],
        "left"
    )
    .select(
        merged_map_entity["*"],
        map_ICentity_df_fixed["IC_Partner_OMNI"],
        map_ICentity_df_fixed["IC_Partner_CODA"]
    )
)

# Step 4: Replace null IC_Partner_CODA with 'Unknown' and update cmpcode for Korea
replaced_values = (
    merged_map_ic_entity
    .withColumn(
        "IC_Partner_CODA_1",
        F.coalesce(F.col("IC_Partner_CODA"), F.lit("Unknown"))
    )
    .withColumn(
        "cmpcode_1",
        F.when(F.col("cmpcode") == "KOREA", F.lit("SOUTH KOREA")).otherwise(F.col("cmpcode"))
    )
)

# Step 5: Add _keyCforia column based on doccode and cForia/InvoiceNumber
added_keyCforia = (
    replaced_values
    .withColumn(
        "_keyCforia",
        F.when(
            F.lower(F.col("doccode")).like("%cancel%"),
            F.concat(F.col("doccode"), F.lit("c"))
        ).otherwise(
            F.concat(F.coalesce(F.col("cForia"), F.lit("")), F.coalesce(F.col("InvoiceNumber"), F.lit("")))
        )
    )
)

# Step 6: Add _key_global_identifier column with custom logic
added_key_global_identifier = (
    added_keyCforia
    .withColumn(
        "_key_global_identifier",
        F.when(
            F.right(
                F.coalesce(F.col("ServiceDeliverySiteInvoiceNumber"), F.lit("")),
                F.lit(1).cast("string")
            ) == "A",
            F.concat(
                F.coalesce(F.col("cmpcode_1"), F.lit("")),
                F.lit("-"),
                F.coalesce(F.col("InvoiceNumber"), F.lit(""))
            )
        )
        .when(
            F.col("ServiceDeliverySiteInvoiceNumber") == "Unknown",
            F.concat(
                F.coalesce(F.col("cmpcode_1"), F.lit("")),
                F.lit("-"),
                F.coalesce(F.col("InvoiceNumber"), F.lit(""))
            )
        )
        .when(
            F.col("ICEntityMapKey") == "WARWAR",
            F.concat(
                F.lit("WAR-"),
                F.coalesce(F.col("ServiceDeliverySiteInvoiceNumber"), F.lit(""))
            )
        )
        .when(
            F.col("InvoiceType") == "IC",
            F.concat(
                F.coalesce(F.col("cmpcode_1"), F.lit("")),
                F.lit("-"),
                F.coalesce(F.col("InvoiceNumber"), F.lit(""))
            )
        )
        .otherwise(
            F.concat(
                F.upper(F.coalesce(F.col("IC_Partner_CODA_1"), F.lit(""))),
                F.lit("-"),
                F.coalesce(F.col("ServiceDeliverySiteInvoiceNumber"), F.lit(""))
            )
        )
    )
)

# Step 7: Add _key_AR_CODA column as SiteID-InvoiceNumber
added_key_AR_CODA = (
    added_key_global_identifier
    .withColumn(
        "_key_AR_CODA",
        F.concat(
            F.coalesce(F.col("SiteID"), F.lit("")),
            F.lit("-"),
            F.coalesce(F.col("InvoiceNumber"), F.lit(""))
        )
    )
)

# Step 8: Rename columns for final output
renamed_columns = (
    added_key_AR_CODA
    .select(
        F.col("SiteID").alias("SiteID"),
        F.col("cmpcode_1").alias("AR Billing Site"),
        F.col("doccode").alias("doccode"),
        F.col("InvoiceDate").alias("Invoice_Date"),
        F.col("InvoiceNumber").alias("AR_Invoice_Number"),
        F.col("DocType").alias("AR_Doc_Type"),
        F.col("InvoiceType").alias("AR_Invoice_Type"),
        F.col("ICEntityMapKey").alias("ICEntityMapKey"),
        F.col("_keyClientID").alias("_keyClientID"),
        F.col("CustomerNumber").alias("AR_Customer_Number"),
        F.col("CustomerName").alias("AR_Customer_Name"),
        F.col("PO").alias("AR_PO"),
        F.col("Protocol").alias("AR_Protocol"),
        F.col("JobNumber").alias("AR_Job_Number"),
        F.col("statpay").alias("Stat_Pay"),
        F.col("valuehome").alias("valuehome"),
        F.col("valuedoc").alias("valuedoc"),
        F.col("ServiceDeliverySite").alias("AR Originating Site"),
        F.col("ServiceDeliverySiteInvoiceNumber").alias("AR_Originating_Invoice"),
        F.col("_InvoiceSite_DeliverySite").alias("_InvoiceSite_DeliverySite"),
        F.col("gpm_site").alias("gpm_site"),
        F.col("cForia").alias("cForia"),
        F.col("IC_Partner_OMNI").alias("IC_Partner_OMNI"),
        F.col("IC_Partner_CODA_1").alias("IC_Partner_CODA"),
        F.col("_keyCforia"),
        F.col("_key_global_identifier"),
        F.col("_key_AR_CODA")
    )
)

# Step 9: Replace 'MAI' with 'HORSHAM' in AR Billing Site and AR Originating Site
final_df = (
    renamed_columns
    .withColumn(
        "AR Billing Site",
        F.regexp_replace(
            F.regexp_replace(F.col("AR Billing Site"), "MAI", "HORSHAM"),
            "MAI",
            "HORSHAM"
        )
    )
    .withColumn(
        "AR Originating Site",
        F.regexp_replace(
            F.regexp_replace(F.col("AR Originating Site"), "MAI", "HORSHAM"),
            "MAI",
            "HORSHAM"
        )
    )
)

# COMMAND ----------

final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save("s3://tfsdl-corp-fdt/test/psg/ctd/cert/aqe_ref/psg_ctd_cert_aqe")

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
