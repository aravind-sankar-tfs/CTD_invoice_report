# Databricks notebook source
# MAGIC %md
# MAGIC ###Imports

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data urls

# COMMAND ----------

mdp_aqe="s3://tfsdl-corp-fdt/test/psg/ctd/cert/aqe_ref/psg_ctd_cert_aqe_mdp_5426"
omni_table_aqe="s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table_aqe"
merged_table="s3://tfsdl-corp-fdt/test/psg/ctd/cert/merged_table"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Data from Sources
# MAGIC - OMNI TABLE AQE
# MAGIC - MDP REVENUE AQE

# COMMAND ----------

try:
    # Read MDP Table
    mdp_df = spark.read.format("delta").load("s3://tfsdl-corp-fdt/test/psg/ctd/cert/aqe_ref/psg_ctd_cert_aqe_mdp_5426")
    # Read OMNI Table
    omni_df = spark.read.format("delta").load("s3://tfsdl-corp-fdt/test/psg/ctd/cert/omni_table_aqe")
except Exception as e:
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Aliases and Perform Join
# MAGIC
# MAGIC - Created aliases for the DataFrames: `m` for `mdp_df` and `o` for `omni_df`.
# MAGIC - Performed a full outer join between `m` and `o` on the condition that `Key_global_identifier` from `mdp_df` matches `key_global_identifier` from `omni_df`.
# MAGIC - The resulting DataFrame, `df_joined`, contains all records from both DataFrames, with matching rows merged and non-matching rows included with nulls for missing values.

# COMMAND ----------


# Create Aliases and Perform Join
m = mdp_df.alias("m")
o = omni_df.alias("o")

df_joined = m.join(o, m["Key_global_identifier"] == o["key_global_identifier"], "full_outer")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expand and Select Columns
# MAGIC
# MAGIC - Selected all columns from the `mdp_df` DataFrame using its alias `m`.
# MAGIC - Selected specific columns from the `omni_df` DataFrame using its alias `o`, and assigned new aliases for clarity:
# MAGIC   - `Currency` as `Currency_o`
# MAGIC   - `Global_Identifier` as `Global_Identifier_o`
# MAGIC   - `InvoiceNumber` as `InvoiceNumber_o`
# MAGIC   - `key_global_identifier` as `key_global_identifier_o`
# MAGIC - Included the `NetValue` and `Quantity` columns from `omni_df`.
# MAGIC - The resulting DataFrame, `df_expanded`, contains all columns from `mdp_df` and selected, renamed columns from `omni_df`.

# COMMAND ----------

# CExpand and Select Columns
df_expanded = df_joined.select(
    *[m[c] for c in m.columns],
    o["Currency"].alias("Currency_o"),
    o["Global_Identifier"].alias("Global_Identifier_o"),
    o["InvoiceNumber"].alias("InvoiceNumber_o"),
    o["key_global_identifier"].alias("key_global_identifier_o"),
    o["NetValue"],
    o["Quantity"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Conversion and Column Merging
# MAGIC
# MAGIC - Converted the `valuedoc` column to `double` type for accurate numerical operations.
# MAGIC - Merged columns from both sources to create unified fields:
# MAGIC   - `InvoiceNumberNew`: Uses `InvoiceNumber_o` if available, otherwise falls back to `InvoiceNumber`.
# MAGIC   - `key_global_identifier_new`: Uses `key_global_identifier_o` if available, otherwise falls back to `Key_global_identifier`.
# MAGIC   - `Global_Identifier_New`: Uses `Global_Identifier_o` if available, otherwise falls back to `Global_Identifier`.
# MAGIC   - `Currency.1`: Uses `curdoc` if available, otherwise falls back to `Currency_o`.
# MAGIC - These steps ensure that the resulting DataFrame has consistent and complete values by prioritizing non-null entries from the joined datasets.

# COMMAND ----------

# Type Conversion and Column Merging
df_typed = df_expanded.withColumn("valuedoc", F.col("valuedoc").cast("double"))

df_merged = (
    df_typed.withColumn(
        "InvoiceNumberNew",
        F.when(F.col("InvoiceNumber_o").isNull(), F.col("InvoiceNumber"))
         .otherwise(F.col("InvoiceNumber_o"))
    )
    .withColumn(
        "key_global_identifier_new",
        F.when(F.col("key_global_identifier_o").isNull(), F.col("Key_global_identifier"))
         .otherwise(F.col("key_global_identifier_o"))
    )
    .withColumn(
        "Global_Identifier_New",
        F.when(F.col("Global_Identifier_o").isNull(), F.col("Global_Identifier"))
         .otherwise(F.col("Global_Identifier_o"))
    )
    .withColumn(
        "Currency.1",
        F.when(F.col("curdoc").isNull(), F.col("Currency_o"))
         .otherwise(F.col("curdoc"))
    )
)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter and Clean Data
# MAGIC
# MAGIC - Filtered the merged DataFrame to include only rows where `key_global_identifier_new` is not null and not an empty string after trimming.
# MAGIC - Filled missing values in the `NetValue`, `Quantity`, and `valuedoc` columns with 0 to ensure data consistency and prevent null-related errors in subsequent analysis.

# COMMAND ----------

# Filter and Clean Data
df_filtered = df_merged.filter(
    (F.col("key_global_identifier_new").isNotNull()) &
    (F.length(F.trim(F.col("key_global_identifier_new"))) > 0)
)

df_filled = df_filtered.fillna({"NetValue": 0, "Quantity": 0, "valuedoc": 0})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variance Calculation and Column Cleanup
# MAGIC
# MAGIC This section performs the following steps:
# MAGIC
# MAGIC 1. **Calculate Variance**  
# MAGIC    Adds a new column `Variance` to the DataFrame, computed as `(valuedoc * -1) - NetValue`. This represents the difference between the negated `valuedoc` and the `NetValue` for each row.
# MAGIC
# MAGIC 2. **Drop Repeated Columns**  
# MAGIC    Removes redundant or repeated columns from the DataFrame, including original and joined versions of invoice numbers, global identifiers, and currency fields, to streamline the dataset for further processing.

# COMMAND ----------

# Calculate Variance
df_variance = df_filled.withColumn("Variance", (F.col("valuedoc") * -1) - F.col("NetValue"))

#drop  repeted columns
drop_cols = [
    "InvoiceNumber", "Key_global_identifier", "Global_Identifier", "curdoc",
    "InvoiceNumber_o", "key_global_identifier_o", "Global_Identifier_o", "Currency_o"
]
df_cleaned = df_variance.drop(*drop_cols)



# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename and Transform Columns
# MAGIC
# MAGIC This section performs the following steps:
# MAGIC
# MAGIC 1. **Rename Columns**  
# MAGIC    - Renames multiple columns in the DataFrame for clarity and consistency
# MAGIC
# MAGIC 2. **Adjust Quantity Values**  
# MAGIC    - Creates a new `Quantity` column:
# MAGIC      - If `NetValue` is not zero, sets `Quantity` to the negative of `QuantityOG`.
# MAGIC      - Otherwise, keeps `Quantity` the same as `QuantityOG`.
# MAGIC
# MAGIC 3. **Type Casting**  
# MAGIC    - Casts the `NetValue` and `Quantity` columns to `double` type to ensure correct numerical operations in further analysis.

# COMMAND ----------

# Rename Columns
df_renamed = (
    df_cleaned.withColumnRenamed("el3", "GLCode")
      .withColumnRenamed("NetValue", "NetValue0")
      .withColumnRenamed("Variance", "NetValue")
      .withColumnRenamed("cmpcode", "Site")
      .withColumnRenamed("CustomerName", "Subsidiary")
      .withColumnRenamed("InvoiceNumberNew", "InvoiceNumber")
      .withColumnRenamed("key_global_identifier_new", "key_global_identifier")
      .withColumnRenamed("Global_Identifier_New", "Global_Identifier")
      .withColumnRenamed("Currency.1", "Currency")
      .withColumnRenamed("Site_GL_CODE", "SiteGLCode")
      .withColumnRenamed("Quantity", "QuantityOG")
)

# Adjust Quantity and Type Casting
df_adjusted = df_renamed.withColumn(
    "Quantity",
    F.when(F.col("NetValue") != 0, F.col("QuantityOG") * -1)
     .otherwise(F.col("QuantityOG"))
)

df_casted = df_adjusted.withColumn("NetValue", F.col("NetValue").cast("double"))
df_casted = df_casted.withColumn("Quantity", F.col("Quantity").cast("double"))

# COMMAND ----------

# Apply Filters on netvalue , netvalue0 and global identifier
df_netvalue_filtered = df_casted.filter(F.abs(F.col("NetValue")) >= 0.01)
df_netvalue0_filtered = df_netvalue_filtered.filter(F.col("NetValue0").isNotNull())
df_final_filtered = df_netvalue0_filtered.filter(F.col("Global_Identifier").isNotNull())

# COMMAND ----------

#Add Description Column
df_merged_table = df_final_filtered.withColumn(
    "Description",
    F.when(F.col("CPQ_Category").isNull(), F.lit("Derived"))
     .otherwise(F.concat_ws(", ", F.lit("Derived:"), F.col("CPQ_Category"), F.col("CPQ_SubCategory")))
)

# COMMAND ----------

try:
    df_merged_table.write.format("delta").mode("overwrite").save(merged_table)
except Exception as e:
    raise e
