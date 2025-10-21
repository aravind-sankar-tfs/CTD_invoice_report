# Databricks notebook source
# MAGIC %md
# MAGIC # PSG OMNI Description Table
# MAGIC
# MAGIC ## Notebook Overview
# MAGIC
# MAGIC This notebook processes **finance invoicing data** from Microsoft Fabric and MDM sources to generate the **OMNI Description Table**.  
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
# MAGIC - OMNI Description Table output:  
# MAGIC `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc`
# MAGIC
# MAGIC ##  Summary
# MAGIC - Combines Fabric invoice data and MDM client master data.
# MAGIC - Maps **sites and SiteIDs** for consistent reporting.
# MAGIC - Computes **derived columns** like global identifiers and formatted invoice dates.
# MAGIC - Aggregates financial values and quantities.
# MAGIC - Produces a clean, ready-to-use **Delta OMNI Description table** for downstream analytics.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports
# MAGIC - Import PySpark functions, exceptions, regex, and utility modules.
# MAGIC - Required for data manipulation, joins, aggregation, and error handling.

# COMMAND ----------

from pyspark.errors import PySparkException
from pyspark.sql import functions as F
from pyspark.sql import functions as F
from itertools import chain

# COMMAND ----------

# MAGIC %md
# MAGIC ### Credentials
# MAGIC - Connects to **Microsoft Fabric Datawarehouse** via Azure Active Directory authentication.
# MAGIC - JDBC URL and connection properties are configured for `Finance - Invoicing` tables.

# COMMAND ----------

# Connect to Microsoft Fabric Datawarehouse using Azure Active Directory authentication
jdbc_url = "jdbc:sqlserver://fvzh3nukvj3upilj5pvxu2r3m4-dhobibgnitienogvfhocprazui.datawarehouse.fabric.microsoft.com:1433;database=GDS - Warehouse"

connection_properties = {
    "authentication": "ActiveDirectoryPassword",
    "user": "CHALL.Omni_ESP@thermofisher.com",
    "password": "Njiyy22bufe3nf2f!oya#8E23sYe",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}



# COMMAND ----------

# MAGIC %md
# MAGIC **MDM Data source**
# MAGIC
# MAGIC - **Variable**: mdm
# MAGIC - S3 Path: `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/master_client_data`
# MAGIC
# MAGIC **Omni Table Output**
# MAGIC
# MAGIC - Variable: **omni_table_output**
# MAGIC - S3 Path: `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table`

# COMMAND ----------

# athena data source
mdm = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/master_client_data"
#output path
omni_desc_output ="s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data from Fabric
# MAGIC - Load invoice headers and lines via **JDBC**.
# MAGIC - Catch PySpark exceptions and provide descriptive error messages.
# MAGIC **Tables:**
# MAGIC - `dbo.Finance - Invoicing - Invoice and Credit Note Headers`
# MAGIC - `dbo.Finance - Invoicing - Invoice and Creadit Note Lines`

# COMMAND ----------

# DBTITLE 1,Headers and Line Items
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

# COMMAND ----------

try:
    df_mdm_client_clients = spark.read.format("delta").load(mdm)
except PySparkException as ex:
    raise ValueError(f"Error Condition: {ex.getErrorClass()}, Message arguments: {ex.getMessageParameters()}, SQLSTATE: {ex.getSqlState()}, {ex}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mapping Site IDs
# MAGIC -  **facility-to-site mapping**: maps `Facility` → `(Site Short Code, SiteID)`.
# MAGIC - Create mappings for:
# MAGIC   - Full name → struct(Site, SiteID)
# MAGIC   - Shortcode → struct(Site, SiteID)
# MAGIC   - Shortcode → SiteID
# MAGIC - Handles fallback scenarios for unmapped or numeric facilities.

# COMMAND ----------


facility_to_siteinfo = {
    'Basel': ('BAS', 1),
    'China - Beijing': ('BEIJING', 7),
    'China - Suzhou': ('SUZHOU', 31),
    'Hegenheimer': ('HEGENHEIMER', 38),
    'Horsham': ('MAI', 2),
    'Mount Prospect': ('MP', 4),
    'Japan': ('TOKYO', 15),
    'Mexico - FCS Mexico City': ('MEXICO', 11),
    'South Korea': ('SEOUL', 33),
    'Colombia - FCS Bogota': ('COLOMBIA', 18),
    'Allentown': ('ALLENTOWN', 0),
    'Singapore': ('SINGAPORE', 3),
    'Peru - FCS Lima': ('PERU', 17),
    'Chile - FCS Santiago': ('CHILE', 16),
    'Argentina - FCS Buenos Aires': ('ARGENTINA', 12),
    'Brazil - FCS Sao Paulo': ('BRAZIL', 9)
}

facility_to_siteinfo_upper = {k.upper(): v for k, v in facility_to_siteinfo.items()}

shortcode_to_siteinfo = {v[0].upper(): (v[0].upper(), v[1]) for v in facility_to_siteinfo.values()}

# Create create_map expressions
map_fullname = F.create_map(
    *list(chain.from_iterable(
        [(F.lit(k), F.struct(F.lit(v[0]).alias("Site"), F.lit(v[1]).alias("SiteID")))
         for k, v in facility_to_siteinfo_upper.items()]
    ))
)

# Shortcode map: SHORTCODE -> struct(Site, SiteID)
map_shortcode = F.create_map(
    *list(chain.from_iterable(
        [(F.lit(k), F.struct(F.lit(v[0]).alias("Site"), F.lit(v[1]).alias("SiteID"))) 
         for k, v in shortcode_to_siteinfo.items()]
    ))
)

map_shortcode_to_id = F.create_map(
    *list(chain.from_iterable(
        [(F.lit(k), F.lit(v[1])) for k, v in shortcode_to_siteinfo.items()]
    ))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculations for OMNI Description Table
# MAGIC
# MAGIC 1. Filter invoice headers for **Invoice Date >= 2019-01-01**.
# MAGIC 2. Join **invoice headers (`ih`)** with **invoice lines (`il`)** using `Global Identifier`.
# MAGIC 3. Left join with **MDM client data (`mc`)** to attach client information.
# MAGIC 4. Clean and uppercase `Facility` and `Business Unit` for consistent mapping.
# MAGIC 5. Map **Site** and **SiteID** using multiple mapping expressions with priority:
# MAGIC    - Full facility name
# MAGIC    - Shortcode
# MAGIC    - Business Unit shortcode
# MAGIC    - Numeric fallback
# MAGIC    - Unknown (`-1`)
# MAGIC 6. Derive additional columns:
# MAGIC    - `InvoiceDate_formatted`: formatted invoice date.
# MAGIC    - `GLCode_upper`: uppercased GL Code.
# MAGIC    - `key_global_identifier`: concatenation of SiteID, Invoice Number, and GL Code.
# MAGIC 7. Aggregate at **Site + Invoice + GL + Record Type** level:
# MAGIC    - Sum `NetValue` and `Quantity`.
# MAGIC 8. Select and rename final columns for the output table.
# MAGIC 9. Order data by `SiteID`, `InvoiceDate`, and `InvoiceNumber`.
# MAGIC

# COMMAND ----------

try:
    df_omni_desc = (
        df_finance_invoicing_headers.alias("ih")
        # Filter early
        .filter(F.col("ih.`Invoice Date`") >= "2019-01-01")
        # Inner join with invoicing lines
        .join(
            df_finance_invoicing_lines.alias("il"),
            F.col("il.`Global Identifier`") == F.col("ih.`Global Identifier`"),
            "inner"
        )
        .join(
            df_mdm_client_clients.alias("mc"),
            F.col("mc.id") == F.concat(F.col("ih.`Business Unit`"), F.lit("-"), F.col("ih.Client")),
            "left"
        )
        .withColumn("Facility_clean", F.upper(F.trim(F.col("Facility"))))
        .withColumn("BusinessUnit_clean", F.upper(F.trim(F.col("ih.`Business Unit`"))))
        .withColumn("site_struct_full", map_fullname[F.col("Facility_clean")])

        .withColumn("site_struct_short", map_shortcode[F.col("Facility_clean")])
        
        .withColumn("site_struct_bu", map_shortcode[F.col("BusinessUnit_clean")])
        
        .withColumn(
            "Site",
            F.when(F.col("site_struct_full").Site.isNotNull(), F.col("site_struct_full").Site)
             .when(F.col("site_struct_short").Site.isNotNull(), F.col("site_struct_short").Site)
             .when(F.col("site_struct_bu").Site.isNotNull(), F.col("site_struct_bu").Site)
             .when(F.col("Facility_clean").rlike("^[0-9]+$"), F.concat(F.lit("SITE_"), F.col("Facility_clean")))
             .otherwise(F.col("Facility_clean"))
        )
        # Determine SiteID (priority: fullname -> shortcode -> BU shortcode -> numeric -> unknown (-1))
        .withColumn(
            "SiteID",
            F.when(F.col("site_struct_full").SiteID.isNotNull(), F.col("site_struct_full").SiteID)
             .when(F.col("site_struct_short").SiteID.isNotNull(), F.col("site_struct_short").SiteID)
             .when(F.col("site_struct_bu").SiteID.isNotNull(), F.col("site_struct_bu").SiteID)
             .when(F.col("Facility_clean").rlike("^[0-9]+$"), F.col("Facility_clean").cast("long"))
             .otherwise(F.lit(-1))
        )
        # Other derived columns
        .withColumn("InvoiceDate_formatted", F.date_format(F.col("ih.`Invoice Date`"), "MM/dd/yyyy"))
        .withColumn("GLCode_upper", F.upper(F.col("il.`GL Code`")))
        .withColumn(
            "key_global_identifier",
            F.upper(
                F.trim(
                    F.concat(F.col("SiteID").cast("string"), F.trim(F.col("il.`Invoice Number`")), F.trim(F.col("il.`GL Code`")))
                )
            )
        )
        # Group by and aggregate
        .groupBy(
            "SiteID",
            "Site",
            F.col("InvoiceDate_formatted").alias("InvoiceDate"),
            F.col("il.`Invoice Number`").alias("InvoiceNumber"),
            F.col("ih.`Record Type`").alias("Record_Type"),
            F.col("ih.`Purchase Order`").alias("PurchaseOrder"),
            F.col("ih.Protocol"),
            F.col("ih.Currency"),
            F.col("GLCode_upper").alias("GLCode"),
            F.col("il.Description"),
            F.col("mc.alpha_name").alias("Subsidiary"),
            F.col("key_global_identifier")
        )
        .agg(
            F.sum("il.`Net Value`").alias("NetValue"),
            F.sum("il.Quantity").alias("Quantity")
        )
        # Final selection and ordering (drop helper columns)
        .select(
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
        )
        .distinct()
        .orderBy("SiteID", "InvoiceDate", "InvoiceNumber")
    )

except Exception as e:
    print("Error while processing df_omni_desc:", str(e))
    df_omni_desc = None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to s3
# MAGIC - Write **OMNI Description Table** as **Delta table** to S3.
# MAGIC - Use `overwrite` mode to refresh the dataset on every run.
# MAGIC - Catch exceptions and raise error messages if write fails.

# COMMAND ----------

try:
    df_omni_desc.write.format("delta").mode("overwrite").save(omni_desc_output)
except Exception as e:
    raise RuntimeError(f"Error writing df_omni_desc to S3: {str(e)}")
