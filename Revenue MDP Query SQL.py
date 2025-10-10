# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

def convert_decimal_value(value_col, dp_col):
    """
    Convert decimal values based on decimal places
    Formula: if dp == 0 then value else value / 10^(dp-2)
    """
    return F.when(F.col(dp_col) == 0, F.col(value_col)) \
            .otherwise(F.col(value_col) / F.pow(F.lit(10), F.col(dp_col) - 2))
            

# COMMAND ----------

def process_psg_cert_revenue():
    try:
        
        # Data sources
        dochead = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_dochead"
        doc_oas_company = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_company"
        docline = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_docline"
        ele1 = "s3://tfsdl-lslpg-fdt-test/psg_ctd_el1"
        ele2 = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el2_element"
        ele3 = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el3_element"
        ele4 = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el4_element"
        
        # Load data
        DH = spark.read.format("delta").load(dochead)
        DL = spark.read.format("delta").load(docline)
        C = spark.read.format("delta").load(doc_oas_company)
        E2 = spark.read.format("delta").load(ele2)
        E3 = spark.read.format("delta").load(ele3)
        E4 = spark.read.format("delta").load(ele4)
        E1 = spark.read.format("csv").option("header", True).option("inferSchema", True).load(ele1)
        
        #1 ARLines: Get customer-related lines with el3
      
        ar_lines = DL.alias("DL").join(
            DH.alias("DH"),
            (F.trim(F.col("DL.cmpcode")) == F.trim(F.col("DH.cmpcode"))) &
            (F.trim(F.col("DL.docnum")) == F.trim(F.col("DH.docnum"))) &
            (F.trim(F.col("DL.doccode")) == F.trim(F.col("DH.doccode"))),
            "inner"
        ).where(
            (
                (F.col("DL.el2").like("021%")) |
                (
                    F.col("DL.cmpcode").isin(['ALLENTOWN', 'MP', 'CLINTRAK', 'INDY']) &
                    (F.col("DL.el2") == "28020")
                )
            ) &
            (F.col("DL.statpay") != 665) &
            (F.col("DH.docdate") >= F.lit("2019-01-01").cast("date"))
        ).select(
            F.col("DL.cmpcode").alias("ar_cmpcode"),
            F.col("DL.doccode").alias("ar_doccode"),
            F.col("DL.docnum").alias("ar_docnum"),
            F.col("DL.el3").alias("ar_el3")
        ).groupBy("ar_cmpcode", "ar_doccode", "ar_docnum", "ar_el3").agg(
            F.first("ar_el3").alias("customer_el3")
        ).select(
            F.col("ar_cmpcode"),
            F.col("ar_doccode"),
            F.col("ar_docnum"),
            F.col("customer_el3")
        )
        
        ar_lines.cache()
        # ar_count = ar_lines.count()
        
        # (4) Base: Join DL+DH and precompute all expressions        
        base = DL.alias("DL2").join(
            DH.alias("DH"),
            (F.trim(F.col("DL2.cmpcode")) == F.trim(F.col("DH.cmpcode"))) &
            (F.trim(F.col("DL2.doccode")) == F.trim(F.col("DH.doccode"))) &
            (F.trim(F.col("DL2.docnum")) == F.trim(F.col("DH.docnum"))),
            "inner"
        ).select(
            F.col("DL2.*"),
            F.col("DH.docdate"),
            F.col("DH.curdoc"),
            
            # Precompute Global_Identifier
            F.concat(
                F.upper(F.col("DL2.cmpcode")),
                F.lit("-"),
                F.when(F.col("DL2.cmpcode").isin(['ALLENTOWN', 'MAI', 'CHINA', 'SINGAPORE', 'CLINTRAK', 'MP']), 
                       F.col("DL2.ref1"))
                .when(F.col("DL2.cmpcode").isin(['JAPAN', 'SUZHOU']), 
                      F.col("DL2.ref5"))
                .otherwise(F.ltrim(F.col("DL2.docnum")))
            ).alias("global_identifier"),
            
            # Precompute InvoiceNumber
            F.when(F.col("DH.cmpcode").isin(['ALLENTOWN', 'MAI', 'CHINA', 'CLINTRAK', 'MP', 'INDY']), 
                   F.col("DL2.ref1"))
            .when(F.col("DH.cmpcode").isin(['BAS', 'WAR', 'HEG']), 
                  F.ltrim(F.col("DH.docnum").cast("string")))
            .when((F.col("DH.cmpcode") == "SINGAPORE") & (F.col("DL2.ref1").like("CN%")), 
                  F.substring(F.col("DL2.ref1"), 3, 999))
            .when(F.col("DH.cmpcode").isin(['KOREA', 'JAPAN', 'SUZHOU']), 
                  F.col("DL2.ref1"))
            .otherwise(F.lit("Unknown"))
            .alias("invoice_number"),
            
            # Precompute PO
            F.when(F.col("DH.cmpcode") == "SUZHOU", F.col("DL2.ref2"))
            .when((F.col("DH.cmpcode") == "WAR") & (F.col("DH.docdate") >= F.lit("2020-11-10").cast("date")), 
                  F.col("DL2.ref3"))
            .when((F.col("DH.cmpcode") == "KOREA") & (F.col("DH.docdate") <= F.lit("2024-07-12").cast("date")), 
                  F.col("DL2.ref2"))
            .when((F.col("DH.cmpcode") == "KOREA") & (F.col("DH.docdate") == F.lit("2024-07-23").cast("date")), 
                  F.col("DL2.ref3"))
            .when((F.col("DH.cmpcode") == "JAPAN") & 
                  ((F.col("DH.docdate") <= F.lit("2024-07-31").cast("date")) | 
                   (F.col("DH.docdate") == F.lit("2024-08-20").cast("date"))), 
                  F.col("DL2.ref2"))
            .otherwise(F.col("DL2.ref4"))
            .alias("po_value"),
            
            # Precompute Protocol
            F.when(F.col("DH.cmpcode") == "SUZHOU", F.col("DL2.ref3"))
            .when((F.col("DH.cmpcode") == "KOREA") & (F.col("DH.docdate") <= F.lit("2024-07-12").cast("date")), 
                  F.col("DL2.ref3"))
            .when((F.col("DH.cmpcode") == "JAPAN") & 
                  ((F.col("DH.docdate") <= F.lit("2024-07-31").cast("date")) | 
                   (F.col("DH.docdate") == F.lit("2024-08-20").cast("date"))), 
                  F.col("DL2.ref3"))
            .otherwise(F.col("DL2.ref2"))
            .alias("protocol_value")
        )
        
        base.cache()
        # base_count = base.count()        
        #  job_defaults: Get default JobNumber per cmpcode (earliest non-blank el4)        
        window_spec = Window.partitionBy("cmpcode").orderBy("docdate")
        
        job_defaults = base.where(
            (F.col("el4").isNotNull()) & (F.col("el4") != "")
        ).withColumn(
            "rn", F.row_number().over(window_spec)
        ).where(
            F.col("rn") == 1
        ).select(
            F.col("cmpcode"),
            F.col("el4").alias("default_el4")
        )
                
        # Excluded doccode list
        excluded_doccodes = [
            'DISPERSE', 'MATCHING', 'PCANCEL', 'PINV', 'PINVDBT', 'RECEIPTS', 
            'Y/E-PROC-BS', 'CPAY', 'CREC', 'OBAL', 'PCRN', 'REVAL', 'REVALR', 
            'YE-PROC-BS', 'PINV_XL', 'PINVDBT_XL', 'PINV2', 'PINV1', 'ACCLTBI', 
            'ACCLTBIREV', 'ACCREV', 'ACCRUAL', 'CORRECTIVE', 'PCRNIC', 'PINVIC', 
            'RECLASS', 'REVACC', 'REVERSAL', 'JGEN', 'JGENREV', 'JREVGEN'
        ]
        
        # Main aggregation - First add customer el3 from ar_lines as a separate column
        base_with_customer = base.alias("b").join(
            ar_lines.alias("a"),
            (F.col("a.ar_cmpcode") == F.col("b.cmpcode")) &
            (F.col("a.ar_doccode") == F.col("b.doccode")) &
            (F.col("a.ar_docnum") == F.col("b.docnum")),
            "inner"
        ).select(
            F.col("b.*"),
            F.col("a.customer_el3")
        )
        
        # Now do the rest of the joins on base_with_customer
        agg = base_with_customer.alias("bwc").join(
            C.alias("C"),
            F.col("C.code") == F.col("bwc.cmpcode"),
            "inner"
        ).join(
            E2.alias("EL2"),
            (F.col("EL2.el2_cmpcode") == F.col("bwc.cmpcode")) &
            (F.col("EL2.el2_code") == F.col("bwc.el2")) &
            (F.col("EL2.el2_elmlevel") == 2),
            "inner"
        ).join(
            E3.alias("CUS"),
            (F.col("CUS.el3_cmpcode") == F.col("bwc.cmpcode")) &
            (F.col("CUS.el3_code") == F.col("bwc.customer_el3")) &
            (F.col("CUS.el3_elmlevel") == 3),
            "inner"
        ).where(
            (
                (F.col("bwc.el2").like("4%")) |
                (F.col("bwc.el2").between("23160", "23180")) |
                ((F.col("bwc.cmpcode").isin(['BAS', 'WAR'])) & (F.col("bwc.el2") == "24300"))
            ) &
            ~((F.col("bwc.cmpcode") == "MAI") & (F.col("bwc.ref2").like("%Recharges%"))) &
            ~((F.col("bwc.cmpcode").isin(['ALLENTOWN', 'MP', 'INDY', 'CLINTRAK'])) & 
              (F.col("bwc.el4").like("X%"))) &
            (~F.col("bwc.doccode").isin(excluded_doccodes)) &
            (F.col("bwc.docdate") >= F.lit("2019-01-01").cast("date"))
        ).groupBy(
            "bwc.cmpcode", "bwc.global_identifier", "bwc.statpay", "bwc.docdate", "bwc.curdoc",
            "bwc.el3", "bwc.customer_el3", "bwc.invoice_number", "bwc.po_value", "bwc.protocol_value",
            "bwc.docnum", "bwc.doccode", "bwc.el2", "bwc.el4", "C.homecur", "EL2.el2_name", "CUS.el3_name"
        ).agg(
            F.sum(convert_decimal_value("bwc.valuehome", "bwc.valuehome_dp")).alias("valuehome"),
            F.sum(convert_decimal_value("bwc.valuedoc", "bwc.valuedoc_dp")).alias("valuedoc")
        ).select(
            F.col("bwc.cmpcode").alias("cmpcode"),
            F.col("bwc.global_identifier").alias("global_identifier"),
            F.col("bwc.statpay").alias("statpay"),
            F.col("bwc.docdate").alias("docdate"),
            F.col("bwc.curdoc").alias("curdoc"),
            F.col("bwc.el3").alias("el3"),
            F.col("bwc.customer_el3").alias("customer_number"),
            F.col("bwc.invoice_number").alias("invoice_number"),
            F.col("bwc.po_value").alias("po_value"),
            F.col("bwc.protocol_value").alias("protocol_value"),
            F.col("bwc.docnum").alias("docnum"),
            F.col("bwc.doccode").alias("doccode"),
            F.col("bwc.el2").alias("el2"),
            F.col("bwc.el4").alias("el4"),
            F.col("C.homecur").alias("valuehomecurrency"),
            F.col("EL2.el2_name").alias("el2_name"),
            F.col("CUS.el3_name").alias("customer_name"),
            F.col("valuehome"),
            F.col("valuedoc")
        )
                
        # Final SELECT with SiteID mapping and JobNumber default
        revnue_mpd = agg.alias("agg").join(
            job_defaults.alias("jd"),
            F.col("agg.cmpcode") == F.col("jd.cmpcode"),
            "left"
        ).select(
            # SiteID mapping
            F.when(F.col("agg.cmpcode") == "ALLENTOWN", F.lit("0"))
            .when(F.col("agg.cmpcode") == "BAS", F.lit("1"))
            .when(F.col("agg.cmpcode") == "MAI", F.lit("2"))
            .when(F.col("agg.cmpcode") == "MP", F.lit("4"))
            .when(F.col("agg.cmpcode") == "WAR", F.lit("13"))
            .when(F.col("agg.cmpcode") == "CLINTRAK", F.lit("99"))
            .when(F.col("agg.cmpcode") == "INDY", F.lit("20"))
            .when(F.col("agg.cmpcode") == "KOREA", F.lit("33"))
            .when(F.col("agg.cmpcode") == "JAPAN", F.lit("15"))
            .when(F.col("agg.cmpcode") == "SINGAPORE", F.lit("3"))
            .when(F.col("agg.cmpcode") == "CHINA", F.lit("7"))
            .when(F.col("agg.cmpcode") == "SUZHOU", F.lit("31"))
            .when(F.col("agg.cmpcode") == "HEG", F.lit("38"))
            .otherwise(F.lit("Unknown"))
            .cast("string")
            .alias("SiteID"),
            
            F.col("agg.cmpcode"),
            F.col("agg.global_identifier").alias("Global_Identifier"),
            F.col("agg.statpay"),
            F.col("agg.docdate").alias("InvoiceDate"),
            F.col("agg.valuehome"),
            F.col("agg.valuehomecurrency").alias("ValueHomeCurrency"),
            F.col("agg.valuedoc"),
            F.col("agg.curdoc"),
            F.col("agg.customer_number").alias("CustomerNumber"),
            F.col("agg.customer_name").alias("CustomerName"),
            F.col("agg.el3"),
            F.col("agg.invoice_number").alias("InvoiceNumber"),
            F.col("agg.po_value").alias("PO"),
            F.col("agg.protocol_value").alias("Protocol"),
            
            # JobNumber with default fallback
            F.coalesce(
                F.when(F.col("agg.el4") != "", F.col("agg.el4")).otherwise(F.lit(None)),
                F.col("jd.default_el4")
            ).alias("JobNumber"),
            
            F.col("agg.docnum"),
            F.col("agg.doccode"),
            F.col("agg.el2"),
            F.col("agg.el2_name").alias("name")
        )    
             
        return revnue_mpd
        
    except Exception as e:
        error_msg = f"Error in process_psg_cert_revenue: {str(e)}"
        print(error_msg)
        raise Exception(error_msg)

# COMMAND ----------

if __name__ == "__main__":
    try:
        df_revnue_mpd = process_psg_cert_revenue()
        output_path = "s3://tfsdl-corp-fdt/test/psg/ctd/cert/revenue_mdp"
        try:
            df_revnue_mpd.write.mode("overwrite").format("delta").save(output_path)
        except Exception as write_err:
            raise Exception(f"Failed to write revenue_mdp to {output_path}: {str(write_err)}")
    except Exception as e:
        raise Exception(f"Failed to execute: {str(e)}")

