# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC
# MAGIC This process transforms and summarizes invoice and document line data to create a comprehensive, business-ready dataset.  
# MAGIC It combines detailed transaction records with company and element information, applies business rules, and calculates key financial and operational metrics.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Sources
# MAGIC
# MAGIC - **Document Lines** (`psg_mydata_production_euw1_raw_erp.coda_oas_docline`): Contains individual transaction details for each invoice.
# MAGIC - **Document Headers** (`psg_mydata_production_euw1_raw_erp.coda_oas_dochead`): Contains summary information for each invoice.
# MAGIC - **Company Reference** (`psg_mydata_production_euw1_raw_erp.coda_oas_company`): Provides company details and mappings.
# MAGIC - **Element Reference** (`psg_mydata_production_euw1_raw_erp.coda_oas_el3_element`): Provides element information.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step-by-Step Business Explanation
# MAGIC
# MAGIC - **Step 1:** Aggregate revenue document lines (`el2` like '021%') per invoice, summing `valuedoc`/`valuehome` and capturing key reference fields.  
# MAGIC - **Step 2:** Pull auxiliary attributes (`el4`) from non‑revenue document lines, join both aggregates to document headers and enrich with company/element lookups.  
# MAGIC - **Step 3:** Apply business rules to derive SiteID, InvoiceNumber, ServiceDeliverySite, filter out excluded statuses/codes, and output the final reporting dataset.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC - **AggregatedDL:** Summarizes financial transaction lines for invoices from `coda_oas_docline` where `el2` starts with '021'.
# MAGIC - **AggregatedDL2:** Extracts additional attributes (e.g., service delivery site) from `coda_oas_docline` where `el2` does not start with '021'.
# MAGIC - **FinalAggregated:** Combines header, line, company, and element data, applies business rules, and produces the final reporting dataset.
# MAGIC - **All tables reference:** `psg_mydata_production_euw1_raw_erp.coda_oas_docline`, `psg_mydata_production_euw1_raw_erp.coda_oas_dochead`, `psg_mydata_production_euw1_raw_erp.coda_oas_company`, `psg_mydata_production_euw1_raw_erp.coda_oas_el3_element`.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Libraries

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assign the source path(S3) to variables

# COMMAND ----------

# DBTITLE 1,Create Variables_S3 path
oas_docline_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_docline"
oas_dochead_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_dochead"
oas_company_str = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_company"
oas_el3_element = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el3_element"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Creating Input Datasets with Source Paths

# COMMAND ----------

# DBTITLE 1,Creating Datasets
# Load docline data from Delta table and select relevant columns
oas_docline_df = (
    spark.read.format("delta")
    .load(oas_docline_str)
    .select(
        "cmpcode",
        "doccode",
        "docnum",
        "statpay",
        "valuedoc",
        "valuehome",
        "valuedoc_dp",
        "valuehome_dp",
        "el2",
        "el3",
        "el4",
        "ref1",
        "ref2",
        "ref3",
        "ref4",
        "ref5"
    )
)

# Load dochead data and select key columns
oas_dochead_df = spark.read.format("delta").load(oas_dochead_str).select("cmpcode","doccode","docdate","docnum")

# Load ompany data and select company code
oas_company_df = spark.read.format("delta").load(oas_company_str).select("code")

# Load element data and select relevant columns
oas_el3_element_df = spark.read.format("delta").load(oas_el3_element).select("el3_name","el3_code","el3_cmpcode","el3_elmlevel")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating `Aggregated Dataset`
# MAGIC - **Input:** `oas_docline_df`
# MAGIC - **Logic:** Aggregates document line records where `el2` starts with '021'.
# MAGIC - **Key Calculations:**
# MAGIC   - Sums `valuedoc` and `valuehome` after scaling by their decimal precision fields (`valuedoc_dp`, `valuehome_dp`).
# MAGIC   - Uses `CASE` to treat null or zero decimal precision as zero value.
# MAGIC   - Picks the maximum value for reference fields (`el2`, `el3`, `ref1`-`ref5`) per group.
# MAGIC - **Grouping:** By `cmpcode`, `doccode`, `docnum`, `statpay`.

# COMMAND ----------

# DBTITLE 1,output: aggregated_df
aggregated_df = (
    oas_docline_df
      .filter(F.col("el2").startswith("021"))
      .select(
          "cmpcode", "doccode", "docnum", "statpay",
          "el2", "el3", "ref1", "ref2", "ref3", "ref4", "ref5",
          (F.when(F.col("valuedoc_dp").isNull() | (F.col("valuedoc_dp") == 0), F.lit(0))
           .otherwise(F.col("valuedoc") / F.pow(10, F.col("valuedoc_dp") - 2))
          ).alias("valuedoc_calc"),
          (F.when(F.col("valuehome_dp").isNull() | (F.col("valuehome_dp") == 0), F.lit(0))
           .otherwise(F.col("valuehome") / F.pow(10, F.col("valuehome_dp") - 2))
          ).alias("valuehome_calc")
      )
      .groupBy("cmpcode", "doccode", "docnum", "statpay")
      .agg(
          F.sum("valuedoc_calc").alias("valuedoc"),
          F.sum("valuehome_calc").alias("valuehome"),
          F.max("el2").alias("el2"),
          F.max("el3").alias("el3"),
          F.max("ref1").alias("ref1"),
          F.max("ref2").alias("ref2"),
          F.max("ref3").alias("ref3"),
          F.max("ref4").alias("ref4"),
          F.max("ref5").alias("ref5")
      )
)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating Dataset `Aggregated Dataset2`-Extracts additional business attributes
# MAGIC - **Input:** `oas_docline_df`
# MAGIC - **Logic:** Aggregates document line records where `el2` does **not** start with '021' and `el4` is not empty.
# MAGIC - **Key Calculation:** Picks the maximum `el4` value per invoice.
# MAGIC - **Grouping:** By `cmpcode`, `doccode`, `docnum`.

# COMMAND ----------

# DBTITLE 1,output: aggregated_df2
aggregated_df2 = (
    oas_docline_df.filter((~F.col("el2").like("021%")) & (F.col("el4") != ""))
      .groupBy("cmpcode", "doccode", "docnum")
      .agg(F.max("el4").alias("el4"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating`FinalAggregated Dataset -Enriching and Mapping Data` 
# MAGIC - **Input:** `oas_dochead`, `aggregated_df, aggregated-df2, oas_company_df and oas_el3_element_df`.
# MAGIC - **Logic:** 
# MAGIC   - Maps company codes to business site IDs.
# MAGIC   - Determines invoice number, type, service delivery site, and other business fields using `CASE` logic.
# MAGIC - **Purpose:** Produces a business-ready, enriched final dataset with all key attributes and mappings applied.
# MAGIC
# MAGIC #### Filtering and Finalizing the Dataset
# MAGIC
# MAGIC - Filters out records with payment status 665, certain document codes, and those with specific keywords in the service delivery site invoice number (ex:'REIMB', 'olidated').

# COMMAND ----------

# DBTITLE 1,output: final_df
final_df = (
    oas_dochead_df.alias("DH")
    .join(
        aggregated_df.alias("DL"),
        (F.trim(F.col("DL.cmpcode")) == F.trim(F.col("DH.cmpcode"))) &
        (F.trim(F.col("DL.doccode")) == F.trim(F.col("DH.doccode"))) &
        (F.trim(F.col("DL.docnum")) == F.trim(F.col("DH.docnum"))),
        "left"
    )
    .join(
        aggregated_df2.alias("DL2"),
        (F.trim(F.col("DL2.cmpcode")) == F.trim(F.col("DL.cmpcode"))) &
        (F.trim(F.col("DL2.doccode")) == F.trim(F.col("DL.doccode"))) &
        (F.trim(F.col("DL2.docnum")) == F.trim(F.col("DL.docnum"))),
        "left"
    )
    .join(
        oas_company_df.alias("COMP"),
        F.col("COMP.code") == F.col("DL.cmpcode"),
        "left"
    )
    .join(
        oas_el3_element_df.alias("ELE"),
        (F.col("ELE.el3_cmpcode") == F.col("DL.cmpcode")) &
        (F.col("ELE.el3_code") == F.col("DL.el3")) &
        (F.col("ELE.el3_elmlevel") == F.lit(3)),
        "left"
    )
    .filter(
        F.col("DH.cmpcode").isin(
            'ALLENTOWN', 'MAI', 'MP', 'BAS', 'WAR', 'CLINTRAK', 'SINGAPORE',
            'INDY', 'HEG', 'SUZHOU', 'CHINA', 'JAPAN', 'KOREA'
        ) &
        (F.col("DL.statpay") != 665) &
        (F.col("DH.docdate") >= F.lit("2019-01-01")) &
        (~F.col("DH.doccode").isin(
            'DISPERSE', 'MATCHING', 'PCANCEL', 'PINV', 'PINVDBT', 'RECEIPTS',
            'Y/E-PROC-BS', 'CPAY', 'CREC', 'OBAL', 'PCRN', 'REVAL', 'REVALR',
            'YE-PROC-BS', 'PINV_XL', 'PINVDBT_XL', 'PINV2', 'PINV1', 'ACCLTBI',
            'ACCLTBIREV', 'ACCREV', 'ACCRUAL', 'CORRECTIVE', 'PCRNIC', 'PINVIC',
            'RECLASS', 'REVACC', 'REVERSAL', 'JGEN', 'JGENREV', 'JREVGEN'
        ))
    )
    .select(
        F.when(F.col("DH.cmpcode") == "ALLENTOWN", F.lit("0"))
         .when(F.col("DH.cmpcode") == "BAS", F.lit("1"))
         .when(F.col("DH.cmpcode") == "MAI", F.lit("2"))
         .when(F.col("DH.cmpcode") == "MP", F.lit("4"))
         .when(F.col("DH.cmpcode") == "WAR", F.lit("13"))
         .when(F.col("DH.cmpcode") == "CLINTRAK", F.lit("99"))
         .when(F.col("DH.cmpcode") == "INDY", F.lit("20"))
         .when(F.col("DH.cmpcode") == "KOREA", F.lit("33"))
         .when(F.col("DH.cmpcode") == "JAPAN", F.lit("15"))
         .when(F.col("DH.cmpcode") == "SINGAPORE", F.lit("3"))
         .when(F.col("DH.cmpcode") == "CHINA", F.lit("7"))
         .when(F.col("DH.cmpcode") == "SUZHOU", F.lit("31"))
         .when(F.col("DH.cmpcode") == "HEG", F.lit("38"))
         .otherwise(F.lit("Unknown"))
         .alias("SiteID"),
        F.col("DH.cmpcode"),
        F.col("DH.doccode"),
        F.col("DH.docdate").alias("InvoiceDate"),
        F.when(F.col("DH.cmpcode").isin('ALLENTOWN', 'MAI', 'CHINA', 'SINGAPORE', 'CLINTRAK', 'MP'), F.col("DL.ref1"))
         .when(F.col("DH.cmpcode").isin('JAPAN', 'SUZHOU'), F.col("DL.ref5"))
         .otherwise(F.ltrim(F.col("DH.docnum"))).alias("InvoiceNumber"),
        F.when(F.col("DH.doccode").like("%CR%"), F.lit("Credit Note"))
         .otherwise(F.lit("Invoice")).alias("DocType"),
        F.upper(
            F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.substring(F.col("DL.ref5"), 1, 6))
             .when((F.col("DH.cmpcode") == "BAS") & (~F.col("DL.ref5").like("PT%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.ref5").like("PT%")), F.substring(F.col("DL.ref5"), 1, 6))
             .when((F.col("DH.cmpcode") == "WAR") & (~F.col("DL.ref5").like("PT%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
             .when((F.col("DH.cmpcode") == "ALLENTOWN") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "MP") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
             .when((F.col("DH.cmpcode") == "MP") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
             .when((F.col("DH.cmpcode") == "CLINTRAK") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
             .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.ref3").like("PT%")), F.substring(F.col("DL.ref3"), 1, 6))
             .when((F.col("DH.cmpcode") == "MAI") & (~F.col("DL.ref3").like("PT%")), F.col("DH.cmpcode"))
             .otherwise("Unknown")
        ).alias("ServiceDeliverySite"),
        F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.expr("substring(DL.ref5, 8, length(DL.ref5))"))
         .when((F.col("DH.cmpcode") == "BAS") & (~F.col("DL.ref5").like("PT%")), F.ltrim(F.col("DH.docnum")))
         .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.ref5").like("PT%")), F.expr("substring(DL.ref5, 8, length(DL.ref5))"))
         .when((F.col("DH.cmpcode") == "WAR") & (~F.col("DL.ref5").like("PT%")), F.ltrim(F.col("DH.docnum")))
         .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL2.el4").like("X%")), F.col("DL.ref5"))
         .when((F.col("DH.cmpcode") == "ALLENTOWN") & (~F.col("DL2.el4").like("X%")), F.col("DL.ref1"))
         .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL2.el4").like("X%")), F.col("DL.ref5"))
         .when((F.col("DH.cmpcode") == "CLINTRAK") & (~F.col("DL2.el4").like("X%")), F.col("DL.ref1"))
         .when((F.col("DH.cmpcode") == "MP") & (F.col("DL2.el4").like("X%")), F.col("DL.ref5"))
         .when((F.col("DH.cmpcode") == "MP") & (~F.col("DL2.el4").like("X%")), F.col("DL.ref1"))
         .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.ref3").like("PT%")), F.expr("substring(DL.ref3, 8, length(DL.ref3))"))
         .when((F.col("DH.cmpcode") == "MAI") & (~F.col("DL.ref3").like("PT%")), F.col("DL.ref1"))
         .otherwise("Unknown")
         .alias("ServiceDeliverySiteInvoiceNumber"),
        F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.el3").like("ICOM%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.el3").like("ICOM%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.ref5").like("PT%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL.el3").like("X%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL2.el4").like("X%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL.el3").like("X%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL2.el4").like("X%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "MP") & (F.col("DL.el3").like("X%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "MP") & (F.col("DL2.el4").like("X%")), F.lit("3PD-PT"))
         .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.el3").like("IF%")), F.lit("IC"))
         .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.ref3").like("PT%")), F.lit("3PD-PT"))
         .otherwise(F.lit("3PD-OWN"))
         .alias("InvoiceType"),
        F.upper(
            F.concat(
                F.col("DH.cmpcode"),
                F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.substring(F.col("DL.ref5"), 1, 6))
                 .when((F.col("DH.cmpcode") == "BAS") & (~F.col("DL.ref5").like("PT%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "WAR") & (F.col("DL.ref5").like("PT%")), F.substring(F.col("DL.ref5"), 1, 6))
                 .when((F.col("DH.cmpcode") == "WAR") & (~F.col("DL.ref5").like("PT%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "ALLENTOWN") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
                 .when((F.col("DH.cmpcode") == "ALLENTOWN") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "MP") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
                 .when((F.col("DH.cmpcode") == "MP") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "CLINTRAK") & (F.col("DL2.el4").like("X%")), F.col("DL2.el4"))
                 .when((F.col("DH.cmpcode") == "CLINTRAK") & (~F.col("DL2.el4").like("X%")), F.col("DH.cmpcode"))
                 .when((F.col("DH.cmpcode") == "MAI") & (F.col("DL.ref3").like("PT%")), F.substring(F.col("DL.ref3"), 1, 6))
                 .when((F.col("DH.cmpcode") == "MAI") & (~F.col("DL.ref3").like("PT%")), F.col("DH.cmpcode"))
                 .otherwise("Unknown")
            )
        ).alias("ICEntityMapKey"),
        F.concat(
            F.when(F.col("DH.cmpcode") == "ALLENTOWN", F.lit("0"))
             .when(F.col("DH.cmpcode") == "BAS", F.lit("1"))
             .when(F.col("DH.cmpcode") == "MAI", F.lit("2"))
             .when(F.col("DH.cmpcode") == "MP", F.lit("4"))
             .when(F.col("DH.cmpcode") == "WAR", F.lit("13"))
             .when(F.col("DH.cmpcode") == "CLINTRAK", F.lit("99"))
             .when(F.col("DH.cmpcode") == "INDY", F.lit("20"))
             .when(F.col("DH.cmpcode") == "KOREA", F.lit("33"))
             .when(F.col("DH.cmpcode") == "JAPAN", F.lit("15"))
             .when(F.col("DH.cmpcode") == "SINGAPORE", F.lit("3"))
             .when(F.col("DH.cmpcode") == "CHINA", F.lit("7"))
             .when(F.col("DH.cmpcode") == "SUZHOU", F.lit("31"))
             .when(F.col("DH.cmpcode") == "HEG", F.lit("38"))
             .otherwise(F.lit("Unknown")),
            F.lit("-"),
            F.col("DL.el3")
        ).alias("_keyClientID"),
        F.col("DL.el3").alias("CustomerNumber"),
        F.col("ELE.el3_name").alias("CustomerName"),
        F.when(F.col("DH.cmpcode") == "CHINA", F.col("DL.ref2"))
         .when(F.col("DH.cmpcode") == "WAR", F.col("DL.ref3"))
         .otherwise(F.col("DL.ref4")).alias("PO"),
        F.when(F.col("DH.cmpcode").isin("JAPAN", "SUZHOU"), F.col("DL.ref3"))
         .otherwise(F.col("DL.ref2")).alias("Protocol"),
        F.when((F.col("DH.cmpcode") == "BAS") & (F.col("DL.ref5").like("PT%")), F.col("DL.ref3"))
         .otherwise(F.coalesce(F.col("DL2.el4"), F.lit(""))).alias("JobNumber"),
        F.col("DL.statpay"),
        F.col("DL.valuehome").cast("double").alias("valuehome"),
        F.col("DL.valuedoc").cast("double").alias("valuedoc")
    )
    .filter(
        (~F.upper(F.coalesce(F.col("ServiceDeliverySiteInvoiceNumber"), F.lit("[]"))).like("%REIMB%")) &
        (~F.lower(F.coalesce(F.col("ServiceDeliverySiteInvoiceNumber"), F.lit("[]"))).like("%olidated%"))
    )
).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save the final DataFrame to S3 in Delta format with overwrite mode:

# COMMAND ----------

# DBTITLE 1,Write_to_S3
final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save("s3://tfsdl-corp-fdt/test/psg/ctd/cert/psg_ctd_cert_ar")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Final Code Validation

# COMMAND ----------

#Validation Checks: 1.schema 2.data 3.duplicates 4.count 5.data in s3
#final_df_dedup.printSchema()
#display(final_df_dedup)

#print(final_df.count())  #1305270
#print(final_df_dedup.count()) #1305193

# #Find duplicates
# final_df.createOrReplaceTempView("final_df_view")
# #final_df_dedup.createOrReplaceTempView("final_df_view")
# columns = final_df.columns
# columns_str = ", ".join([f"`{col}`" for col in columns])
# query = f"""
#     SELECT {columns_str}, COUNT(*) as count
#     FROM final_df_view
#     GROUP BY {columns_str}
#     HAVING COUNT(*) > 1
# """
# duplicate_records_df = spark.sql(query)
# display(duplicate_records_df)

# Validate data in S3
# df = spark.read.format("delta").load("s3://tfsdl-corp-fdt/test/psg/ctd/cert/psg_ctd_cert_ar")
# df.count()
# display(df)
