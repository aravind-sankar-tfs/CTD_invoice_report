# Databricks notebook source
# MAGIC %md
# MAGIC - ### Pull the data from MDP
# MAGIC - ### Apply transformations
# MAGIC - ### Load the data into S3
# MAGIC

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load data from MDP

# COMMAND ----------

# DBTITLE 1,Creating Dfs
oas_docline_df = (
    spark.read.format("delta")
    .load("s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_docline")
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
oas_dochead_df = spark.read.format("delta").load("s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_dochead").select("cmpcode","doccode","docdate","docnum")
oas_company_df = spark.read.format("delta").load("s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_company").select("code")
oas_el3_element_df = spark.read.format("delta").load("s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el3_element").select("el3_name","el3_code","el3_cmpcode","el3_elmlevel")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

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

aggregated_df2 = (
    oas_docline_df.filter((~F.col("el2").like("021%")) & (F.col("el4") != ""))
      .groupBy("cmpcode", "doccode", "docnum")
      .agg(F.max("el4").alias("el4"))
)

# COMMAND ----------

#display(aggregated_df)
#display(aggregated_df2)

# COMMAND ----------

siteid_map = {
    'ALLENTOWN': '0', 'BAS': '1', 'MAI': '2', 'MP': '4', 'WAR': '13', 'CLINTRAK': '99',
    'INDY': '20', 'KOREA': '33', 'JAPAN': '15', 'SINGAPORE': '3', 'CHINA': '7',
    'SUZHOU': '31', 'HEG': '38'
}

def siteid_expr(col):
    return F.coalesce(
        F.create_map([F.lit(x) for x in sum(siteid_map.items(), ())])[col],
        F.lit("Unknown")
    )

# COMMAND ----------

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
        siteid_expr(F.col("DH.cmpcode")).alias("SiteID"),
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
            F.col("DH.cmpcode") +
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
        ).alias("ICEntityMapKey"),
        (siteid_expr(F.col("DH.cmpcode")) + F.lit("-") + F.col("DL.el3")).alias("_keyClientID"),
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
        F.col("DL.valuehome"),
        F.col("DL.valuedoc")
    )
    .filter(
        (~F.upper(F.coalesce(F.col("ServiceDeliverySiteInvoiceNumber"), F.lit("[]"))).like("%REIMB%")) &
        (~F.lower(F.coalesce(F.col("ServiceDeliverySiteInvoiceNumber"), F.lit("[]"))).like("%olidated%"))
    )
)

# COMMAND ----------

final_df_dedup=final_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load final data into S3

# COMMAND ----------

final_df_dedup.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .save("s3://tfsdl-corp-fdt/test/psg/ctd/cert/psg_ctd_cert_ar")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Validations

# COMMAND ----------

# # Validation Checks: 1.schema 2.data 3.duplicates
# #final_df.printSchema()
# #display(final_df)

# #print(final_df.count())  --1305270
# #print(final_df_dedup.count()) --1305193

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

#Validate data in S3
#df = spark.read.format("delta").load("s3://tfsdl-corp-fdt/test/psg/ctd/cert/psg_ctd_cert_ar")
#df.count()
#display(df)
