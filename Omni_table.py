# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ### **Notebook Overview**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **1. Load Data Sources**
# MAGIC - **Fabric:**  
# MAGIC   - `df_finance_invoicing_headers`  
# MAGIC   - `df_finance_invoicing_lines`
# MAGIC - **Athena:**  
# MAGIC   - `df_mdm_client_clients`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **2. Map Site IDs to Site Names**
# MAGIC
# MAGIC <div style="width:100%; overflow-x:auto;">
# MAGIC <table style="width:100%; table-layout:fixed; border-collapse: collapse; text-align:left;">
# MAGIC   <thead>
# MAGIC     <tr style="background-color:#f2f2f2;">
# MAGIC       <th style="padding:8px; width:15%;">SiteID</th>
# MAGIC       <th style="padding:8px; width:85%;">Site</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr><td style="padding:8px;">3</td><td style="padding:8px;">Singapore</td></tr>
# MAGIC     <tr><td style="padding:8px;">2</td><td style="padding:8px;">Horsham</td></tr>
# MAGIC     <tr><td style="padding:8px;">31</td><td style="padding:8px;">China - Suzhou</td></tr>
# MAGIC     <tr><td style="padding:8px;">4</td><td style="padding:8px;">Mount Prospect</td></tr>
# MAGIC     <tr><td style="padding:8px;">15</td><td style="padding:8px;">Japan</td></tr>
# MAGIC     <tr><td style="padding:8px;">11</td><td style="padding:8px;">Mexico - FCS Mexico City</td></tr>
# MAGIC     <tr><td style="padding:8px;">33</td><td style="padding:8px;">South Korea</td></tr>
# MAGIC     <tr><td style="padding:8px;">18</td><td style="padding:8px;">Colombia - FCS Bogota</td></tr>
# MAGIC     <tr><td style="padding:8px;">38</td><td style="padding:8px;">Hegenheimer</td></tr>
# MAGIC     <tr><td style="padding:8px;">0</td><td style="padding:8px;">Allentown</td></tr>
# MAGIC     <tr><td style="padding:8px;">7</td><td style="padding:8px;">China - Beijing</td></tr>
# MAGIC     <tr><td style="padding:8px;">17</td><td style="padding:8px;">Peru - FCS Lima</td></tr>
# MAGIC     <tr><td style="padding:8px;">16</td><td style="padding:8px;">Chile - FCS Santiago</td></tr>
# MAGIC     <tr><td style="padding:8px;">1</td><td style="padding:8px;">Basel</td></tr>
# MAGIC     <tr><td style="padding:8px;">12</td><td style="padding:8px;">Argentina - FCS Buenos Aires</td></tr>
# MAGIC     <tr><td style="padding:8px;">9</td><td style="padding:8px;">Brazil - FCS Sao Paulo</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **3. Calculate Required Columns for OMNI Description Table**
# MAGIC - Apply business logic to compute required fields based on [OMNI Table SQL](https://thermofisher.sharepoint.com/:t:/s/FinanceDigitalTransformationTeam/EY61OCJLNslAvJ3CplKxZk4BXyKuOx_LpaZJIDUxvpbj2w?e=HPt1xq).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **4. Export Final DataFrame**
# MAGIC - Save `df_omni_table_final` to S3 at the specified path:  
# MAGIC   `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table`
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.errors import PySparkException
import re
from pyspark.sql import functions as F
from itertools import chain

# COMMAND ----------

# MAGIC %md
# MAGIC ### Credentials

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

#athena datasource
mdm = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/master_client_data"
#omni table output
omni_table_output ="s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper Fucntions

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
# MAGIC ### Load data from fabric

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
# MAGIC ### Load data from s3 (mdm)

# COMMAND ----------

try:
    df_mdm_client_clients = spark.read.format("delta").load(mdm)
except PySparkException as ex:
    raise ValueError(f"Error Condition: {ex.getErrorClass()}, Message arguments: {ex.getMessageParameters()}, SQLSTATE: {ex.getSqlState()}, {ex}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Site Mapping

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
# MAGIC ### Calculation

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
# MAGIC ### Write to s3

# COMMAND ----------

try:
    df_omni_table_final = clean_colnames_s3(df_omni_table)
    df_omni_table_final.write.mode("overwrite").format("delta").save(omni_table_output)
except PySparkException as ex:
    raise ValueError(f"Error Condition: {ex.getErrorClass()}, Message arguments: {ex.getMessageParameters()}, SQLSTATE: {ex.getSqlState()}, {ex}")
