# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Overview
# MAGIC
# MAGIC ## Workflow Steps
# MAGIC
# MAGIC 1. **Helper Functions:**  
# MAGIC    - Build folder URL  
# MAGIC    - Access token  
# MAGIC    - File URL
# MAGIC 2. **SharePoint Credentials:**  
# MAGIC    - Client ID  
# MAGIC    - Driver ID  
# MAGIC    - Client secret (fetched from Databricks scope)
# MAGIC 3. **Data URLs:**  
# MAGIC    - **Source:** `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc`
# MAGIC    - **Destination:** `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc_aqe`
# MAGIC 4. **Read Static Table (SharePoint):**  
# MAGIC    - Loaded static table from SharePoint  
# MAGIC    - Applied column aliasing for join and category columns  
# MAGIC    - File: `z_InvoicingDcode.xlsx`
# MAGIC 5. **Read OMNI Description from S3:**  
# MAGIC    - Loaded OMNI description data from S3 bucket  
# MAGIC    - Casted columns: `InvoiceDate`, `NetValue`, `Quantity`  
# MAGIC    - Generated `Global_Identifier` column  
# MAGIC    - Computed `SiteGlCode` column  
# MAGIC    - Standardized column names for consistency
# MAGIC 6. **Join OMNI Desc with Static Table:**  
# MAGIC    - Merged OMNI description data with static table
# MAGIC .
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installs

# COMMAND ----------

# MAGIC %pip install msal

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

import msal
import urllib.parse
import requests
import pandas as pd
import io
from pyspark.sql.functions import col, concat, lit, to_date
from pyspark.sql.types import DoubleType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper functions

# COMMAND ----------

# Token and URL Builders
def build_url(driveId, path, fileName):
    prefix = "https://graph.microsoft.com/v1.0/drives/"
    if not path.endswith("/"):
        path += "/"
    full_path = f"{path}{fileName}"
    encoded_path = urllib.parse.quote(full_path)
    return f"{prefix}{driveId}/{encoded_path}:/content"

def build_folder_url(driveId, path):
    prefix = "https://graph.microsoft.com/v1.0/drives/"
    return f"{prefix}{driveId}{path}:/children"

def get_token(secret, clientId):
    authority_url = "https://login.microsoftonline.com/b67d722d-aa8a-4777-a169-ebeb7a6a3b67"
    app = msal.ConfidentialClientApplication(
        authority=authority_url,
        client_id=clientId,
        client_credential=secret
    )
    token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    return token
 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Share Point Credentials

# COMMAND ----------

client_id = 'ffdd77e0-b8f0-4c02-879e-51c41bbcd0fa'
driver_id=dbutils.secrets.get('ds-lsg-fids', 'fids_sharepoint_driverID')+'/root:' #sharepoint driver id plus root folder
client_secret =dbutils.secrets.get('ds-lsg-fids', 'fids_azure_sharepoint_app_secret')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Urls

# COMMAND ----------

#source
omni_desc_aqe_source ="s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc"

#destination
omni_desc_aqe_destination= "s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc_aqe"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Static Table (Sharepoint)

# COMMAND ----------

# List files in SharePoint folder using Microsoft Graph API
folder_path = "/CTD"
folder_url = build_folder_url(driver_id, folder_path)
token = get_token(client_secret, client_id)
headers = {"Authorization": f"Bearer {token['access_token']}"}
response = requests.get(folder_url, headers=headers)
if response.status_code == 200:
    files = [item['name'] for item in response.json().get('value', [])]
else:
    raise Exception(f"Failed to list files: {response.status_code} {response.text}")

# COMMAND ----------

# Read z_InvoicingDecode.xlsx directly from SharePoint into a dataframe
file_name = "z_InvoicingDcode.xlsx"
file_url = build_url(driver_id, folder_path, file_name)
file_response = requests.get(file_url, headers=headers)
if file_response.status_code == 200:
    excel_bytes = file_response.content
    try:
        df_static_table = pd.read_excel(io.BytesIO(excel_bytes), sheet_name="Sheet1", dtype=str, engine='openpyxl')
        df_static_table = spark.createDataFrame(df_static_table)
    except Exception as e:
        raise Exception(f"Error reading Excel file: {e}")
else:
    raise Exception(f"Failed to download file: {file_response.status_code} {file_response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read OMNI desc from S3

# COMMAND ----------

try:
    df_omni_desc = spark.read.format("delta").load(omni_desc_aqe_source)
    # display(df_omni_desc)
except Exception as e:
    raise Exception(f"Error reading Delta table from S3: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Required columns selected

# COMMAND ----------

selected_columns = [
    "SiteID",
    "Site",
    "InvoiceDate",
    "InvoiceNumber",
    "Record_Type",
    "PurchaseOrder",
    "Protocol",
    "Currency",
    "GLCode",
    "Description",
    "NetValue",
    "Quantity",
    "Subsidiary",
    "key_global_identifier"
]

df_omni_desc = df_omni_desc[selected_columns]


# COMMAND ----------

# MAGIC %md
# MAGIC **Data type Casting :**
# MAGIC - InvoiceDate to date
# MAGIC - NetValue    to double
# MAGIC - Quantity    to double

# COMMAND ----------

#convert columns to correct data types
df_omni_desc = df_omni_desc\
    .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd/MM/yyyy")) \
    .withColumn("NetValue", col("NetValue").cast(DoubleType())) \
    .withColumn("Quantity", col("Quantity").cast(DoubleType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculation for Global_Identifier column

# COMMAND ----------

#Global_identifier column  = concat --> Site + InvoiceNumber
df_omni_desc = df_omni_desc.withColumn(
    "Global_Identifier",
    concat(col("Site"), lit("-"), col("InvoiceNumber"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculation for SiteGlCode

# COMMAND ----------

#StiteGLCode = concat --> Site + GLCode
df_omni_desc = df_omni_desc.withColumn(
    "SiteGLCode",
    concat(col("Site"), col("GLCode"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns from Static table

# COMMAND ----------

#prepare static table for join
df_static_table = df_static_table.select(
    col("Facility|GLCode-CPQREF").alias("join_key"),   # rename Facility|GLCode-CPQREF as join_key
    col("AI Enhanced - CPQ Category").alias("CPQ_Category"), # rename AI Enhanced - CPQ Category as CPQ_Category
    col("AI Enhanced - CPQ SubCategory").alias("CPQ_SubCategory") # rename AI Enhanced - CPQ SubCategory as CPQ_SubCategory
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join OMNI Desc with Static table

# COMMAND ----------

#join omni Desc with static table to get CPQ Category and SubCategory
df_omni_desc_aqe = df_omni_desc.join(
    df_static_table,
    df_omni_desc["SiteGLCode"] == df_static_table["join_key"],
    "left"
).drop("join_key")


# COMMAND ----------

# MAGIC %md
# MAGIC ### calculation of Source column 
# MAGIC - value : Invoicing 

# COMMAND ----------

df_omni_desc_aqe = df_omni_desc_aqe.withColumn("Source", lit("Invoicing"))

# COMMAND ----------

try:
    # Write DataFrame to S3 in Delta format
    df_omni_desc_aqe.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(omni_desc_aqe_destination)
except Exception as e:
    print(f"Error writing Delta table: {e}")

