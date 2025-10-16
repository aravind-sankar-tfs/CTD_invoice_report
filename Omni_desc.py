# Databricks notebook source
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
# MAGIC - Apply business logic to compute required fields based on the ([OMNI Desc SQL](https://thermofisher.sharepoint.com/:t:/s/FinanceDigitalTransformationTeam/EXTJG0BKPj5LuXn9cEp8N4MBkQWL2Z9P23A2UV7pZiUSBQ?e=p0DJUd)).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **4. Export Final DataFrame**
# MAGIC - Save `df_omni_desc` to S3 at the specified path:  
# MAGIC   `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc`
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.errors import PySparkException
from pyspark.sql import functions as F
from pyspark.sql import functions as F
from itertools import chain

# COMMAND ----------

# MAGIC %md
# MAGIC ### Credentials

# COMMAND ----------

# Connect to Microsoft Fabric Datawarehouse using Azure Active Directory authentication
jdbc_url = "jdbc:sqlserver://fvzh3nukvj3upilj5pvxu2r3m4-dhobibgnitienogvfhocprazui.datawarehouse.fabric.microsoft.com:1433;database=GDS - Warehouse"
# jdbc_url = "jdbc:sqlserver://fvzh3nukvj3upilj5pvxu2r3m4-3g7rhhmom6oefcs3o5op3kikom.datawarehouse.fabric.microsoft.com:1433;database=GDS - Warehouse"


connection_properties = {
    "authentication": "ActiveDirectoryPassword",
    "user": "CHALL.Omni_ESP@thermofisher.com",
    "password": "Njiyy22bufe3nf2f!oya#8E23sYe",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# athena data source
mdm = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/master_client_data"
#output path
omni_desc_output ="s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Headers and line items from fabric

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
# MAGIC ### Load MDM Client data from athena

# COMMAND ----------

try:
    df_mdm_client_clients = spark.read.format("delta").load(mdm)
except PySparkException as ex:
    raise ValueError(f"Error Condition: {ex.getErrorClass()}, Message arguments: {ex.getMessageParameters()}, SQLSTATE: {ex.getSqlState()}, {ex}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mapping Site ID's

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
# MAGIC ### Calculations

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

# COMMAND ----------

try:
    df_omni_desc.write.format("delta").mode("overwrite").save(omni_desc_output)
except Exception as e:
    raise RuntimeError(f"Error writing df_omni_desc to S3: {str(e)}")
