# Databricks notebook source
# MAGIC %md
# MAGIC # PSG OMNI  Table
# MAGIC
# MAGIC ##Notebook Overview
# MAGIC
# MAGIC This notebook processes **finance invoicing data** from Microsoft Fabric and MDM sources to generate the **OMNI  Table**.  
# MAGIC
# MAGIC It combines invoice headers, invoice lines, and client master data to compute required business metrics, map sites, and generate a **ready-to-use Delta table** for reporting and analytics.  
# MAGIC
# MAGIC The notebook ensures consistency in column names, handles site mappings, derives global identifiers, and aggregates financial values at the **invoice and site level**.
# MAGIC
# MAGIC **Key Steps:**
# MAGIC 1. Load data sources from Fabric and S3 (MDM).
# MAGIC 2. Map site IDs to site names and short codes.
# MAGIC 3. Calculate derived fields based on OMNI business logic.
# MAGIC 4. Aggregate and transform data.
# MAGIC 5. Export the final table to S3.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Sources and Paths
# MAGIC
# MAGIC | Source | DataFrame | Description | Path |
# MAGIC |--------|-----------|-------------|------|
# MAGIC | Fabric | `df_finance_invoicing_headers` | Invoice and credit note headers | JDBC: `[dbo].[Finance - Invoicing - Invoice and Credit Note Headers]` |
# MAGIC | Fabric | `df_finance_invoicing_lines` | Invoice and credit note line items | JDBC: `[dbo].[Finance - Invoicing - Invoice and Credit Note Lines]` |
# MAGIC | S3 / Delta | `df_mdm_client_clients` | Client master data (MDM) | `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/master_client_data` |
# MAGIC
# MAGIC **Destination Path:**
# MAGIC - OMNI table output:  
# MAGIC `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports
# MAGIC - Import PySpark functions, exceptions, regex, and utility modules.
# MAGIC - Required for data manipulation, joins, aggregation, and error handling.

# COMMAND ----------

from pyspark.errors import PySparkException
import re
from pyspark.sql import functions as F
from itertools import chain

# COMMAND ----------

# MAGIC %md
# MAGIC ### Credentials
# MAGIC - Connects to **Microsoft Fabric Datawarehouse** via Azure Active Directory authentication.
# MAGIC - JDBC URL and connection properties are configured for `Finance - Invoicing` tables.
# MAGIC

# COMMAND ----------

# Connect to Microsoft Fabric Datawarehouse using Azure Active Directory authentication
jdbc_url = "jdbc:sqlserver://fvzh3nukvj3upilj5pvxu2r3m4-dhobibgnitienogvfhocprazui.datawarehouse.fabric.microsoft.com:1433;database=DW_Platinum_Prod"

connection_properties = {
    "authentication": "ActiveDirectoryPassword",
    "user": "CHALL.Omni_ESP@thermofisher.com",
    "password": "Njiyy22bufe3nf2f!oya#8E23sYe",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# COMMAND ----------

# MAGIC %md
# MAGIC ### MDM Data source
# MAGIC - **Variable:** `mdm`
# MAGIC - **S3 Path:** `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/master_client_data`
# MAGIC
# MAGIC ### Omni Table Output
# MAGIC - **Variable:** `omni_table_output`
# MAGIC - **S3 Path:** `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table`

# COMMAND ----------

#athena datasource
mdm = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/master_client_data"
#omni table output
omni_table_output ="s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Helper functions
# MAGIC `clean_colnames_s3()`
# MAGIC - Ensures DataFrame column names are **S3-friendly**:
# MAGIC   - Converts to lowercase.
# MAGIC   - Replaces spaces/special characters with underscores.
# MAGIC   - Removes non-alphanumeric characters.
# MAGIC   - Trims leading/trailing underscores.

# COMMAND ----------

def clean_colnames_s3(df):
    """
    Clean all column names in a Spark DataFrame to be S3-friendly:
    - Lowercase
    - Replace spaces and special characters with underscores
    - Remove non-alphanumeric/underscore characters
    - Remove leading/trailing underscores
    """
    return df.toDF(*[
        re.sub(r'^_+|_+$', '', re.sub(r'[^a-z0-9]+', '_', col.lower()))
        for col in df.columns
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data from Fabric
# MAGIC - Load invoice headers and lines via **JDBC**.
# MAGIC - Catch PySpark exceptions and provide descriptive error messages.
# MAGIC **Tables:**
# MAGIC - `dbo.Finance - Invoicing - Invoice and Credit Note Headers`
# MAGIC - `dbo.Finance - Invoicing - Invoice and Creadit Note Lines`

# COMMAND ----------

try:
    df_finance_invoicing_headers = spark.read.jdbc(
        url=jdbc_url,
        table="[dbo].[Finance - Invoicing - Invoice and Credit Note Headers]",
        properties=connection_properties
    )

    df_finance_invoicing_lines = spark.read.jdbc(
        url=jdbc_url,
        table="[dbo].[Finance - Invoicing - Invoice and Credit Note Lines]",
        properties=connection_properties
    )
except PySparkException as ex:
    raise ValueError(f"Error Condition: {ex.getErrorClass()}, Message arguments: {ex.getMessageParameters()}, SQLSTATE: {ex.getSqlState()}, {ex}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data from S3 (MDM)
# MAGIC - Load client master data as Delta table.
# MAGIC - Catch PySpark exceptions for error handling.
# MAGIC - Delta Path for MDM:`s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/master_client_data`
# MAGIC

# COMMAND ----------

try:
    df_mdm_client_clients = spark.read.format("delta").load(mdm)
except PySparkException as ex:
    raise ValueError(f"Error Condition: {ex.getErrorClass()}, Message arguments: {ex.getMessageParameters()}, SQLSTATE: {ex.getSqlState()}, {ex}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Site Mapping
# MAGIC - **facility-to-site mapping** dictionary:  
# MAGIC   Maps `Facility` → `(Site Short Code, SiteID)`.
# MAGIC - Convert mapping to PySpark `create_map` expression.
# MAGIC - Enables easy mapping of **Site** and **SiteID** columns for invoices.

# COMMAND ----------

#Define Facility -> (Site Short Code, SiteID) mapping
facility_to_siteinfo = {
    'Basel': ('BAS', 1),
    'China - Beijing': ('CHINA', 7),
    'China - Suzhou': ('SUZHOU', 31),
    'Hegenheimer': ('HEG', 38),
    'Horsham': ('MAI', 2),
    'Mount Prospect': ('MP', 4)
}

# Convert mapping to PySpark create_map expression
mapping_expr = F.create_map(
    *list(chain.from_iterable(
        [(F.lit(k), F.struct(F.lit(v[0]).alias("SiteShort"), F.lit(v[1]).alias("SiteID"))) 
         for k, v in facility_to_siteinfo.items()]
    ))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculation of OMNI Table
# MAGIC
# MAGIC 1. Filter invoice headers for **Invoice Date >= 2019-01-01**.
# MAGIC 2. Join **invoice headers (`ih`)** with **invoice lines (`il`)** using `Global Identifier`.
# MAGIC 3. Left join with **MDM client data (`mc`)** to attach client information.
# MAGIC 4. Map **Site** and **SiteID** using the site mapping expression:
# MAGIC    - Fallback to upper-case `Facility` or Business Unit if unmapped.
# MAGIC 5. Derive additional columns:
# MAGIC    - `InvoiceNumber_trimmed`: Trimmed invoice number.
# MAGIC    - `key_global_identifier`: Uppercase concatenation of invoice number and GL code.
# MAGIC    - `Global_Identifier_Final`: Concatenation of Site and invoice number.
# MAGIC 6. Aggregate at **SiteID + Invoice** level:
# MAGIC    - Sum `NetValue` and `Quantity`.
# MAGIC 7. Select and rename final columns for the output table.
# MAGIC 8. Order data by `SiteID` and `InvoiceNumber`.

# COMMAND ----------

try:
    # Transformation
    df_omni_table = (
        df_finance_invoicing_headers.alias("ih")
        .filter(F.col("ih.`Invoice Date`") >= "2019-01-01")
        .join(
            df_finance_invoicing_lines.alias("il"),
            F.col("il.`Global Identifier`") == F.col("ih.`Global Identifier`"),
            "inner"
        )
        # Left join with client data
        .join(
            df_mdm_client_clients.alias("mc"),
            F.col("mc.id") == F.concat(
                F.col("ih.`Business Unit`"), 
                F.lit("-"), 
                F.col("ih.Client")
            ),
            "left"
        )
        # Map Site & SiteID (fallback for unmapped sites)
        .withColumn("site_struct", mapping_expr[F.col("Facility")])
        .withColumn("Site", F.coalesce(F.col("site_struct.SiteShort"), F.upper(F.col("Facility"))))
        .withColumn("SiteID", F.coalesce(F.col("site_struct.SiteID"), F.col("ih.`Business Unit`")))

        # Derived columns
        .withColumn("InvoiceNumber_trimmed", F.trim(F.col("il.`Invoice Number`")))
        .withColumn(
            "key_global_identifier",
            F.upper(
                F.trim(
                    F.concat(
                        F.trim(F.col("il.`Invoice Number`")),
                        F.trim(F.col("il.`GL Code`"))
                    )
                )
            )
        )
        .withColumn(
            "Global_Identifier_Final",
            F.concat(
                F.col("Site"),
                F.lit("-"),
                F.col("InvoiceNumber_trimmed")
            )
        )
        # Group by and aggregate
        .groupBy(
            "SiteID",
            "InvoiceNumber_trimmed",
            "key_global_identifier",
            "Global_Identifier_Final",
            F.col("ih.Currency")
        )
        .agg(
            F.sum("il.`Net Value`").alias("NetValue"),
            F.sum("il.Quantity").alias("Quantity")
        )
        # Select final columns
        .select(
            "SiteID",
            F.col("InvoiceNumber_trimmed").alias("InvoiceNumber"),
            "key_global_identifier",
            F.col("Global_Identifier_Final").alias("Global_Identifier"),
            "Currency",
            "NetValue",
            "Quantity"
        )
        .distinct()
        .orderBy("SiteID", "InvoiceNumber")
    )
except PySparkException as ex:
    raise ValueError(f"Error Condition: {ex.getErrorClass()}, Message arguments: {ex.getMessageParameters()}, SQLSTATE: {ex.getSqlState()}, {ex}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to S3
# MAGIC - Clean column names using `clean_colnames_s3`.
# MAGIC - Write **OMNI table** as a **Delta table** to S3.
# MAGIC - Use `overwrite` mode to refresh the dataset on every run.
# MAGIC - Catch PySpark exceptions for error handling.
# MAGIC - OMNI TABLE: `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table`

# COMMAND ----------

try:
    df_omni_table_final = clean_colnames_s3(df_omni_table)
    df_omni_table_final.write.mode("overwrite").format("delta").save(omni_table_output)
except PySparkException as ex:
    raise ValueError(f"Error Condition: {ex.getErrorClass()}, Message arguments: {ex.getMessageParameters()}, SQLSTATE: {ex.getSqlState()}, {ex}")
