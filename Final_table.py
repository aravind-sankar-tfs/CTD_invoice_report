# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Overview
# MAGIC
# MAGIC This notebook processes data from two source tables:
# MAGIC
# MAGIC - **omni_desc_table**: `s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc_aqe`
# MAGIC - **merged_table**: `s3://tfsdl-corp-fdt/test/psg/ctd/cert/merged_table`
# MAGIC
# MAGIC ## Steps
# MAGIC
# MAGIC 1. **Read Data**  
# MAGIC    Data is loaded from the above sources.
# MAGIC
# MAGIC 2. **Remove Extra Columns**  
# MAGIC    The following columns are dropped:  
# MAGIC    - `statpay`
# MAGIC    - `ValueHomeCurrency`
# MAGIC    - `MAP_Entity_CODA`
# MAGIC    - `CustomerNumber`
# MAGIC    - `JobNumber`
# MAGIC    - `Pass_Through_Flag`
# MAGIC    - `Pass_Through_Flag2`
# MAGIC    - `PO`
# MAGIC    - `Protocol`
# MAGIC    - `NetValue0`
# MAGIC    - `valuehome`
# MAGIC    - `valuedoc`
# MAGIC    - `QuantityOG`
# MAGIC
# MAGIC 3. **Rename Columns**  
# MAGIC    Columns are renamed in both tables before union:  
# MAGIC    - `CPQ_Category` → `Invoice_CPQ_Category`
# MAGIC    - `CPQ_SubCategory` → `Invoice_CPQ_SubCategory`
# MAGIC
# MAGIC 4. **Union and Deduplication**  
# MAGIC -    The two data sources are unioned and duplicates are dropped to create the final table.
# MAGIC -    **final table:** `s3://tfsdl-corp-fdt/test/psg/ctd/cert/final_table`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.errors import PySparkException

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Urls

# COMMAND ----------

#source tables
omni_desc_table ="s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_desc_aqe"
merged_table="s3://tfsdl-corp-fdt/test/psg/ctd/cert/merged_table"
final_table="s3://tfsdl-corp-fdt/test/psg/ctd/cert/final_table"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data from sources

# COMMAND ----------

try:
    # Read the tables from S3
    omni_desc = spark.read.format("delta").load(omni_desc_table)
    merged = spark.read.format("delta").load(merged_table)
except PySparkException as e:
    raise RuntimeError(f"Error reading tables: {e.getMessage()}") from e

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Columns from Merged table

# COMMAND ----------

#Remove columns from Merged table
columns_to_remove = [
    "statpay", 
    "ValueHomeCurrency", 
    "MAP_Entity_CODA", 
    "CustomerNumber", 
    "JobNumber", 
    "Pass_Through_Flag", 
    "Pass_Through_Flag2", 
    "PO", 
    "Protocol", 
    "NetValue0", 
    "valuehome", 
    "valuedoc", 
    "QuantityOG"
]

merged_cleaned = merged.drop(*columns_to_remove)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename columns before Join

# COMMAND ----------

#  Rename columns in both tables before union
# OMNI Desc aqe renames
omni_desc_renamed = omni_desc \
    .withColumnRenamed("CPQ_Category", "Invoice_CPQ_Category") \
    .withColumnRenamed("CPQ_SubCategory", "Invoice_CPQ_SubCategory")

# Merged renames
merged_renamed = merged_cleaned \
    .withColumnRenamed("CPQ_Category", "Invoice_CPQ_Category") \
    .withColumnRenamed("CPQ_SubCategory", "Invoice_CPQ_SubCategory")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union two tables and Drop Duplicates

# COMMAND ----------

# Union the two tables
source = omni_desc_renamed.unionByName(merged_renamed, allowMissingColumns=True)

#Remove duplicates
final_table = source.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write final df to S3
# MAGIC `s3://tfsdl-corp-fdt/test/psg/ctd/cert/final_table`

# COMMAND ----------

try:
    final_table.write.format("delta").mode("overwrite").save("s3://tfsdl-corp-fdt/test/psg/ctd/cert/final_table")
except Exception as e:
    raise e
