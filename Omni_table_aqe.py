# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Overview
# MAGIC
# MAGIC This notebook demonstrates the following workflow:
# MAGIC
# MAGIC 1. **Read Source Data**  
# MAGIC    Load the Omni table from S3:  
# MAGIC    `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table`
# MAGIC
# MAGIC 2. **Select Required Columns**  
# MAGIC    - `siteid`
# MAGIC    - `global_identifier`
# MAGIC    - `invoicenumber`
# MAGIC    - `key_global_identifier`
# MAGIC    - `netvalue`
# MAGIC    - `quantity`
# MAGIC
# MAGIC 3. **Cast Columns to Double**  
# MAGIC    - `netvalue`
# MAGIC    - `quantity`
# MAGIC
# MAGIC 4. **Filter Data**  
# MAGIC    Exclude rows where `global_identifier` contains "FSC".
# MAGIC
# MAGIC 5. **Save Final Data**  
# MAGIC    Write the filtered data to:  
# MAGIC    `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table_aqe`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Locations

# COMMAND ----------

#source
omni_table_aqe_source = "s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table"
#destination
omni_table_aqe_destination = "s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table_aqe"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load OMNI TABLE from S3

# COMMAND ----------

# Option 1: Read from Delta path
try:
    df_omni_table = spark.read.format("delta").load(omni_table_aqe_source)
except Exception as e:
    print(f"Error loading Delta table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selected Requuired Data Columns

# COMMAND ----------

#Select only required columns 
df_omni_table_selected = df_omni_table.select(
    "siteid",
    "global_identifier",
    "invoicenumber",
    "key_global_identifier",
    "netvalue",
    "quantity",
    "currency"
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cast Columns in to Double
# MAGIC - **netvalue**
# MAGIC - **quantity**

# COMMAND ----------

#Change data types to ensure numeric types
df_omni_table_aqe = df_omni_table_selected \
    .withColumn("netvalue", col("netvalue").cast(DoubleType())) \
    .withColumn("quantity", col("quantity").cast(DoubleType()))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter out FSC from Global _Identifier

# COMMAND ----------

# Filter out rows containing "FCS" in Global_Identifier
df_omni_table_aqe = df_omni_table_aqe.filter(~col("Global_Identifier").contains("FCS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to s3

# COMMAND ----------

try:
    # Write DataFrame to S3 in Delta format
    df_omni_table_aqe.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(omni_table_aqe_destination)
except Exception as e:
    raise RuntimeError(f"Error writing Delta table: {e}")

