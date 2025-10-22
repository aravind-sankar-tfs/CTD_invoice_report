# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ## Summary & Purpose
# MAGIC
# MAGIC This notebook processes the `Final DataFrame' to identify and handle duplicate and unique invoice records based on the `_key_global_identifier` column.
# MAGIC
# MAGIC **Key steps**
# MAGIC - **Duplicate detection**: Flags rows where `AR_Invoice_Type` = "pass‑through" and more than one `AR_Invoice_Number` exists within the same group.  
# MAGIC - **Key reassignment**: For flagged duplicate rows, creates a new unique key by concatenating `AR Billing Site` and `AR_Invoice_Number`.  
# MAGIC - **Unique handling**: Leaves rows that are already unique with their original key.  
# MAGIC - **Union & validation**: Combines the transformed duplicate rows with the untouched unique rows and verifies that no duplicate keys remain.
# MAGIC
# MAGIC **Purpose**
# MAGIC - Ensure each invoice record has a distinct identifier.  
# MAGIC - Enable downstream analytics and reporting on a clean, deduplicated dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Libraries

# COMMAND ----------

# DBTITLE 1,Importing Libraries
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Input Datasets

# COMMAND ----------

# DBTITLE 1,Creating Input Datasets
# # Load Delta tables for PSG CTD Cert AR and final table
# df_psg_ctd_cert_aqe = spark.read.format("delta").load("s3://tfsdl-corp-fdt/test/psg/ctd/cert/psg_ctd_cert_ar")
# df_psg_ctd_cert_final = spark.read.format("delta").load("s3://tfsdl-corp-fdt/test/psg/ctd/cert/final_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Identify duplicate and unique rows in `dataframe` based on `_key_global_identifier`.
# MAGIC
# MAGIC - For duplicates: Flags rows where `AR_Invoice_Type` is "pass-through" and there are multiple `AR_Invoice_Number` per group.
# MAGIC - Flagged rows: Assigns a new key by combining `AR Billing Site` and `AR_Invoice_Number`.
# MAGIC - Unique rows: Retain their original key.
# MAGIC - The code unions flagged and unique rows, then checks for any remaining duplicates in the new key.
# MAGIC #### Note: 
# MAGIC - Challenge is need to check if the required columns are prersent in the dataframe as per the logic developed below if not have to adjust the columns.

# COMMAND ----------

# DBTITLE 1,Validation Checks
# Identify duplicate and unique rows based on '_key_global_identifier'

# Window spec for partitioning by '_key_global_identifier'
w_dup = Window.partitionBy("_key_global_identifier")

# DataFrame A: rows where '_key_global_identifier' is duplicated --replace final_df with actual dataframe
df_a = final_df.withColumn("dup_count", F.count("*").over(w_dup)).filter(F.col("dup_count") > 1)

# DataFrame B: rows where '_key_global_identifier' is unique --replace final_df with actual dataframe
df_b = final_df.withColumn("dup_count", F.count("*").over(w_dup)).filter(F.col("dup_count") == 1).drop("dup_count")

# Add a flag to DataFrame A: 'Y' if 'AR_Invoice_Type' is 'pass-through' and there are multiple 'AR_Invoice_Number' per group, else 'N'
w_invnum = Window.partitionBy("_key_global_identifier")
df_a_flagged = df_a.withColumn(
    "Flag",
    F.when(
        (F.col("AR_Invoice_Type") == "pass-through") &
        (F.count("AR_Invoice_Number").over(w_invnum).cast("int") > 1),
        "Y"
    ).otherwise("N")
)

# For flagged rows, create a new key by formatting 'AR Billing Site' and 'AR_Invoice_Number'; otherwise, keep the original key
df_a_flagged_newkey = df_a_flagged.withColumn(
    "new_key_global_identifier",
    F.when(
        F.col("Flag") == "Y",
        F.format_string("%s-%s", F.col("AR Billing Site").cast("string"), F.col("AR_Invoice_Number").cast("string"))
    ).otherwise(F.col("_key_global_identifier"))
)

# Prepare DataFrame for union: flagged rows with new key, and unique rows with original key
df_a_flagged_y = df_a_flagged_newkey.filter(F.col("Flag") == "Y").drop("Flag", "dup_count")
df_b_newkey = df_b.withColumn("new_key_global_identifier", F.col("_key_global_identifier"))

# Union the two DataFrames
final_union_df = df_b_newkey.unionByName(df_a_flagged_y)

# Check for any remaining duplicate 'new_key_global_identifier'
dup_check = final_union_df.groupBy("new_key_global_identifier").count().filter(F.col("count") > 1)
display(dup_check)
