# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC
# MAGIC This workflow processes client data by joining, aggregating, ranking, and transforming records to produce a deduplicated client key mapping. The main steps are:
# MAGIC
# MAGIC 1. **Inputs:**  
# MAGIC    - `mdm_master_client_data` (mmcd): Master client records.
# MAGIC    - `mdm_xref_client_data` (mxcd): Cross-reference client records.
# MAGIC
# MAGIC 2. **Transformations:**  
# MAGIC    - **Left Join:**  
# MAGIC      Join `mmcd` and `mxcd` on `m_code` to combine client and cross-reference data.
# MAGIC    - **Window Aggregation:**  
# MAGIC      For each (`client_ultimate_parent_id`, `global_ultimate_business_name`), count the number of `c_code` values (`parent_row_count`).
# MAGIC    - **Ranking:**  
# MAGIC      For each `client_ultimate_parent_id`, rank groups by `parent_row_count` (descending) using `DENSE_RANK()`.
# MAGIC    - **Filter:**  
# MAGIC      Keep only rows with the top rank (`parent_row_rank = 1`).
# MAGIC    - **ClientKey Construction:**  
# MAGIC      Build `ClientKey` by extracting the numeric prefix from `site_id`, concatenating with `-`, and appending `c_code`.
# MAGIC
# MAGIC 3. **Output:**  
# MAGIC    - Select distinct records with columns:  
# MAGIC      - `m_code`
# MAGIC      - `client_ultimate_parent_id`
# MAGIC      - `ClientKey`
# MAGIC      - `global_ultimate_business_name`
# MAGIC
# MAGIC 4. **Result:**  
# MAGIC    - The final deduplicated mapping is displayed, showing the most relevant client key per parent group.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Libraries

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assign the source path(S3) to variables

# COMMAND ----------

# DBTITLE 1,Create Variables_S3 path
# S3 Delta path for master client data
mdm_master_client_data = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/master_client_data"

# S3 Delta path for cross-reference client data
mdm_xref_client_data = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/mdm/xref_client_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Input Datasets with Source Paths

# COMMAND ----------

# DBTITLE 1,Creating Input Datasets
# Load the master client data from the specified S3 Delta path into a Spark DataFrame
mdm_master_client_df = spark.read.format("delta").load(mdm_master_client_data)

# Load the cross-reference client data from the specified S3 Delta path into a Spark DataFrame
mdm_xref_client_df = spark.read.format("delta").load(mdm_xref_client_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Client Data Aggregation and Key Mapping Workflow
# MAGIC
# MAGIC 1. **Joining:** Master and cross-reference client data are joined on `m_code`.
# MAGIC 2. **Aggregating:** For each parent group (`client_ultimate_parent_id`, `global_ultimate_business_name`), related cross-reference records are counted as `parent_row_count`.
# MAGIC 3. **Ranking:** Each parent group is ranked by `parent_row_count` to identify the most significant group.
# MAGIC 4. **Filtering:** Only the top-ranked parent group per `client_ultimate_parent_id` is kept.
# MAGIC 5. **ClientKey Construction:** A `ClientKey` is built by extracting the numeric prefix from `site_id` and combining it with `c_code`.
# MAGIC 6. **Selecting Output:** A deduplicated mapping of client keys with relevant parent and business name information is produced.

# COMMAND ----------

# DBTITLE 1,Client Lookup Dataset
# Join mdm_master_client_df and mdm_xref_client_df
joined_df = mdm_master_client_df.alias("mmcd").join(
    mdm_xref_client_df.alias("mxcd"),
    F.col("mmcd.m_code") == F.col("mxcd.m_code"),
    "left"
)

# Window for parent_row_count
parent_count_window = Window.partitionBy(
    "mmcd.client_ultimate_parent_id", "mmcd.global_ultimate_business_name"
)

# Add a column 'parent_row_count' to count the number of c_code values per (client_ultimate_parent_id, global_ultimate_business_name) group
counts_df = joined_df.withColumn(
    "parent_row_count",
    F.count("mxcd.c_code").over(parent_count_window)
).select(
    F.col("mmcd.client_ultimate_parent_id").alias("parent_id"),
    F.col("mmcd.global_ultimate_business_name").alias("parent_name"),
    F.col("parent_row_count")
)

# Window for parent_row_rank
rank_window = Window.partitionBy("parent_id").orderBy(F.desc("parent_row_count"))

# Add a dense rank column to rank parent groups by row count within each parent_id
ranked_df = counts_df.distinct().withColumn(
    "parent_row_rank",
    F.dense_rank().over(rank_window)
)

# Join mmcd, mxcd, and ranked as per SQL
final_df = mdm_master_client_df.alias("mmcd") \
    .join(mdm_xref_client_df.alias("mxcd"), F.col("mmcd.m_code") == F.col("mxcd.m_code"), "left") \
    .join(
        ranked_df.alias("ranked"),
        (F.col("ranked.parent_id") == F.col("mmcd.client_ultimate_parent_id")) &
        (F.col("ranked.parent_row_rank") == F.lit(1)),
        "left"
    )

# Construct the 'ClientKey' column by concatenating:
# - The numeric prefix of 'site_id' (extracted using regex)
# - A hyphen
# - The string value of 'c_code'
client_key_col = F.concat(
    F.regexp_extract(F.col("mxcd.site_id"), '^([0-9]+)', 1),
    F.lit('-'),
    F.col("mxcd.c_code").cast("string")
)

# Select relevant columns and construct the result DataFrame
result_df = final_df.select(
    F.col("mmcd.m_code").cast("integer"),  # Master code
    F.col("mmcd.client_ultimate_parent_id").cast("integer"),  # Ultimate parent ID
    client_key_col.alias("ClientKey"),  # Constructed ClientKey column
    F.col("mmcd.global_ultimate_business_name")  # Ultimate parent business name
).distinct()  # Remove duplicate rows

# COMMAND ----------

# MAGIC %md
# MAGIC ### Code Validations
# MAGIC
# MAGIC - Confirm aggregation and ranking steps produce expected row counts.
# MAGIC - Check `ClientKey` construction logic for correct formatting.
# MAGIC - Verify output columns: `m_code`, `client_ultimate_parent_id`, `ClientKey`, `global_ultimate_business_name`.

# COMMAND ----------

# # Display the result DataFrame
# display(result_df)

# # Print the schema of the result DataFrame
# result_df.printSchema()

# # Print the count of rows in the result DataFrame
# print(result_df.count())

# # Display rows where m_code equals "100006"
# result_df.filter(F.col("m_code")=="100006").display()

# # Register the result DataFrame as a temporary view for SQL queries
# result_df.createOrReplaceTempView("result_view")

# # Find duplicate rows based on key columns and count them
# duplicates_df = spark.sql("""
#     SELECT m_code, client_ultimate_parent_id, ClientKey, global_ultimate_business_name, COUNT(*) AS duplicate_count
#     FROM result_view
#     GROUP BY m_code, client_ultimate_parent_id, ClientKey, global_ultimate_business_name
#     HAVING COUNT(*) > 1
# """)

# # Display the duplicates DataFrame
# display(duplicates_df)
