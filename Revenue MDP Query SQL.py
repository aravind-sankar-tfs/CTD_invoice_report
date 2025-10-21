# Databricks notebook source
# MAGIC %md
# MAGIC #Revenue MDP 
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook Loads the data from multiple sources from S3 and after doing the detailed calculation the final dataset is created and loaded into s3.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Sources
# MAGIC | Dataset                | Description                                                             | S3 Path                                                                              |
# MAGIC | ---------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
# MAGIC | **DH (Document Head)** | Header-level details of documents such as document date, currency, etc. | `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_dochead`     |
# MAGIC | **DL (Document Line)** | Line-level details of transactions within each document.                | `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_docline`     |
# MAGIC | **C (Company)**        | Contains company-level metadata like home currency, company code, etc.  | `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_company`     |
# MAGIC | **E2 (Element 2)**     | Second-level financial element reference data.                          | `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el2_element` |
# MAGIC | **E3 (Element 3)**     | Third-level financial element reference data (customers).               | `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el3_element` |
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Output
# MAGIC
# MAGIC The processed Revenue MDP dataset is stored as a **Delta table** in S3:  
# MAGIC
# MAGIC `s3://tfsdl-corp-fdt/test/psg/ctd/cert/revenue_mdp`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Columns Used
# MAGIC
# MAGIC | Purpose | Columns | Description |
# MAGIC |---------|---------|-------------|
# MAGIC | **Primary Join (DL ↔ DH)** | `cmpcode`, `doccode`, `docnum` | Links document header and line-level records. |
# MAGIC | **Join with AR lines** | `cmpcode`, `doccode`, `docnum` | Attaches customer (`el3`) information to document lines. |
# MAGIC | **Join with Company (C)** | `cmpcode ↔ C.code` | Adds company-level details like home currency. |
# MAGIC | **Join with Element 2 (E2)** | `cmpcode`, `el2` | Links line items with element-level 2 info. |
# MAGIC | **Join with Element 3 (E3)** | `cmpcode`, `customer_el3 ↔ el3_code` | Brings customer-level metadata. |
# MAGIC | **Join with Job Defaults** | `cmpcode` | Retrieves default job number (`el4`) when missing. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 1: Import Dependencies
# MAGIC
# MAGIC The notebook uses **PySpark** libraries for DataFrame operations, transformations, aggregation, and window functions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 2: Define Source and Output Paths
# MAGIC
# MAGIC All source datasets and output location are defined using S3 paths for later loading. The output path stores the final **Delta table**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 3: Helper Functions
# MAGIC
# MAGIC ### Decimal Handling
# MAGIC - A helper function ensures accurate decimal calculations across datasets with inconsistent decimal precision.
# MAGIC - Logic: if decimal places = 0 → use the value as-is; else → scale value by `10^(decimal_places-2)`.
# MAGIC
# MAGIC ### Data Loading
# MAGIC - A loader function reads all source datasets (`DH`, `DL`, `C`, `E2`, `E3`) as Spark DataFrames from S3.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 4: Processing Logic
# MAGIC
# MAGIC ### 4.1 Load Source Data
# MAGIC - Loads datasets either from input arguments or from S3.
# MAGIC - Ensures all subsequent transformations operate on the correct DataFrames.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.2 Derive AR Lines (Customer Information)
# MAGIC - Filters **Document Line + Document Head** to create AR lines containing **customer element (`el3`)**.
# MAGIC - **Filters Applied:**
# MAGIC   - `el2` starting with `021` or specific company-based inclusion (`28020` for some companies).
# MAGIC   - Exclude records where `statpay = 665`.
# MAGIC   - Only include documents dated after **2019-01-01**.
# MAGIC - Aggregates lines to get **unique customer numbers** per document.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.3 Base Dataset (DL + DH Join)
# MAGIC - Joins **Document Line** and **Document Head** using `cmpcode`, `doccode`, and `docnum`.
# MAGIC - Adds computed columns:
# MAGIC   - **Global Identifier**: Unique key combining company code and document references, standardized for each company.
# MAGIC   - **Invoice Number**: Extracted based on company-specific rules.
# MAGIC   - **PO Value**: Purchase order reference extracted from different fields depending on company and date.
# MAGIC   - **Protocol Value**: Protocol/reference information extracted conditionally per company.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.4 Job Defaults Extraction
# MAGIC - Uses a **window function** to get the **first valid `el4`** (job number) per company.
# MAGIC - Provides a **default fallback** for missing `el4` values in later joins.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.5 Excluded Document Codes
# MAGIC - Certain document types are **excluded** from revenue calculations to avoid duplications or non-revenue entries.  
# MAGIC - Examples: reversals, receipts, accruals, corrective postings, and special processing documents.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.6 Join with Customer, Company, and Element Data
# MAGIC - Joins base dataset with:
# MAGIC   - **AR lines** for customer numbers.
# MAGIC   - **Company table** for home currency and company info.
# MAGIC   - **Element 2 table** for financial classification.
# MAGIC   - **Element 3 table** for customer metadata.
# MAGIC - Applies filters:
# MAGIC   - Include specific `el2` patterns relevant for revenue.
# MAGIC   - Exclude recharge-related entries for specific companies.
# MAGIC   - Exclude lines where `el4` starts with `X` for selected companies.
# MAGIC   - Only include documents post **2019-01-01**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.7 Aggregation
# MAGIC - Aggregates **financial values** (`valuehome`, `valuedoc`) per combination of:
# MAGIC   - Company, document, customer, invoice, elements, currency.
# MAGIC - Applies **decimal normalization** for accurate sums.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4.8 Final Column Selection and Mapping
# MAGIC - Maps `cmpcode` to **SiteID** based on predefined company-to-ID mapping.
# MAGIC - Fills missing `JobNumber` using **default job numbers** extracted earlier.
# MAGIC - Selects and renames columns for final output:
# MAGIC   - `SiteID`, `Company`, `Global Identifier`, `InvoiceDate`, `CustomerNumber`, `CustomerName`, `InvoiceNumber`, `PO`, `Protocol`, `JobNumber`, `ValueHome`, `ValueDoc`, etc.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Step 5: Write Output
# MAGIC - Writes the final **PSG Certification Revenue** DataFrame to the defined **Delta table path** in S3.
# MAGIC - Uses **overwrite mode** to ensure the latest run replaces old data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  Summary
# MAGIC - The notebook ensures **clean, normalized, and aggregated revenue data** for revenue mdp
# MAGIC - Handles:
# MAGIC   - Multi-source joins
# MAGIC   - Customer and element metadata
# MAGIC   - Decimal normalization
# MAGIC   - specific logic for invoice, PO, and protocol values
# MAGIC   - Default fallback for missing job numbers
# MAGIC - Provides a **ready-to-use Delta dataset** for financial reporting and analytics.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Imports
# MAGIC
# MAGIC The following libraries are imported for data processing:
# MAGIC
# MAGIC - `SparkSession`: Entry point to Spark functionality.
# MAGIC - `functions as F`: Provides access to built-in PySpark SQL functions.
# MAGIC - `Window`: Enables window operations for advanced aggregations.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC ### Source Paths
# MAGIC
# MAGIC The following S3 paths are used to load the source datasets:
# MAGIC
# MAGIC - **Document Head (DH):**  
# MAGIC   `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_dochead`
# MAGIC - **Company (C):**  
# MAGIC   `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_company`
# MAGIC - **Document Line (DL):**  
# MAGIC   `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_docline`
# MAGIC - **Element 2 (E2):**  
# MAGIC   `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el2_element`
# MAGIC - **Element 3 (E3):**  
# MAGIC   `s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el3_element`

# COMMAND ----------

#Source Paths
dochead = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_dochead"
doc_oas_company = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_company"
docline = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_docline"
ele2 = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el2_element"
ele3 = "s3://psg-mydata-production-euw1-raw/restricted/operations/erp/coda/oas_el3_element"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Output Path
# MAGIC
# MAGIC **The final processed Revenue MDP dataset is saved as a Delta table at:**
# MAGIC
# MAGIC `s3://tfsdl-corp-fdt/test/psg/ctd/cert/revenue_mdp`

# COMMAND ----------

#Output Path
revenue_mdp_output= "s3://tfsdl-corp-fdt/test/psg/ctd/cert/revenue_mdp"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions
# MAGIC  `convert_decimal_value(value_col, dp_col)`
# MAGIC Converts a value to its correct decimal representation based on the number of decimal places.  
# MAGIC - If `dp_col` is 0, returns the value as-is.
# MAGIC - Otherwise, divides the value by 10 to the power of (`dp_col` - 2).
# MAGIC
# MAGIC `load_psg_cert_revenue_sources()`
# MAGIC Loads required Delta tables for PSG Cert Revenue processing.  
# MAGIC Returns: DataFrames for document header, document line, company, element2, and element3.

# COMMAND ----------

#Handle decimal values
def convert_decimal_value(value_col, dp_col):
    """
    Convert decimal values based on decimal places
    Formula: if dp == 0 then value else value / 10^(dp-2)
    """
    return F.when(F.col(dp_col) == 0, F.col(value_col)) \
            .otherwise(F.col(value_col) / F.pow(F.lit(10), F.col(dp_col) - 2))

#Load Data from sources
def load_psg_cert_revenue_sources():
    """
    Loads all required data sources for PSG Cert Revenue processing.
    Returns: DH, DL, C, E2, E3 DataFrames
    """
    try:
        DH = spark.read.format("delta").load(dochead)
        DL = spark.read.format("delta").load(docline)
        C = spark.read.format("delta").load(doc_oas_company)
        E2 = spark.read.format("delta").load(ele2)
        E3 = spark.read.format("delta").load(ele3)
        return DH, DL, C, E2, E3
    except Exception as e:
        raise Exception(f"Error loading PSG Cert Revenue sources: {str(e)}")
            

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculations
# MAGIC
# MAGIC #### `process_psg_cert_revenue` Function
# MAGIC
# MAGIC This function processes and aggregates PSG Certification Revenue data from multiple sources, applying business rules and transformations to produce a clean, ready-to-use dataset.
# MAGIC
# MAGIC **Key Steps:**
# MAGIC
# MAGIC 1. **Data Loading:**  
# MAGIC    Loads Document Head, Document Line, Company, Element2, and Element3 datasets if not provided.
# MAGIC
# MAGIC 2. **AR Lines Extraction:**  
# MAGIC    Identifies customer-related document lines (`el3`) based on business filters (e.g., `el2` patterns, status, date).
# MAGIC
# MAGIC 3. **Base Dataset Construction:**  
# MAGIC    Joins Document Line and Document Head, computes key fields:
# MAGIC    - `global_identifier`: Unique document key
# MAGIC    - `invoice_number`, `po_value`, `protocol_value`: Extracted per company/date logic
# MAGIC
# MAGIC 4. **Job Defaults:**  
# MAGIC    Determines the default job number (`el4`) per company for fallback.
# MAGIC
# MAGIC 5. **Exclusion Logic:**  
# MAGIC    Excludes specific document codes and lines based on business rules.
# MAGIC
# MAGIC 6. **Customer, Company, and Element Joins:**  
# MAGIC    Enriches the base dataset with customer, company, and element metadata.
# MAGIC
# MAGIC 7. **Aggregation:**  
# MAGIC    Sums financial values (`valuehome`, `valuedoc`) with decimal normalization, grouped by key fields.
# MAGIC
# MAGIC 8. **Final Mapping:**  
# MAGIC    Maps company codes to `SiteID`, fills missing job numbers, and selects/renames output columns.
# MAGIC
# MAGIC 9. **Output:**  
# MAGIC    Returns the final aggregated DataFrame ready for writing to the Delta table.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,ru

def process_psg_cert_revenue(DH=None, DL=None, C=None, E2=None, E3=None):
    try:
        # Load data 
        if DH is None or DL is None or C is None or E2 is None or E3 is None:
            DH, DL, C, E2, E3 = load_psg_cert_revenue_sources()
        
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
        
        #Base: Join DL+DH and precompute all expressions        
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
            F.concat(
                F.upper(F.col("DL2.cmpcode")),
                F.lit("-"),
                F.when(F.col("DL2.cmpcode").isin(['ALLENTOWN', 'MAI', 'CHINA', 'SINGAPORE', 'CLINTRAK', 'MP']), 
                       F.col("DL2.ref1"))
                .when(F.col("DL2.cmpcode").isin(['JAPAN', 'SUZHOU']), 
                      F.col("DL2.ref5"))
                .otherwise(F.ltrim(F.col("DL2.docnum")))
            ).alias("global_identifier"),
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
        #exclude doccodes    
        excluded_doccodes = [
            'DISPERSE', 'MATCHING', 'PCANCEL', 'PINV', 'PINVDBT', 'RECEIPTS', 
            'Y/E-PROC-BS', 'CPAY', 'CREC', 'OBAL', 'PCRN', 'REVAL', 'REVALR', 
            'YE-PROC-BS', 'PINV_XL', 'PINVDBT_XL', 'PINV2', 'PINV1', 'ACCLTBI', 
            'ACCLTBIREV', 'ACCREV', 'ACCRUAL', 'CORRECTIVE', 'PCRNIC', 'PINVIC', 
            'RECLASS', 'REVACC', 'REVERSAL', 'JGEN', 'JGENREV', 'JREVGEN'
        ]
        
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
        #select final columns    
        revnue_mdp = agg.alias("agg").join(
            job_defaults.alias("jd"),
            F.col("agg.cmpcode") == F.col("jd.cmpcode"),
            "left"
        ).select(
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
            F.coalesce(
                F.when(F.col("agg.el4") != "", F.col("agg.el4")).otherwise(F.lit(None)),
                F.col("jd.default_el4")
            ).alias("JobNumber"),
            F.col("agg.docnum"),
            F.col("agg.doccode"),
            F.col("agg.el2"),
            F.col("agg.el2_name").alias("name")
        )    
             
        return revnue_mdp
        
    except Exception as e:
        error_msg = f"Error in process_psg_cert_revenue: {str(e)}"
        raise Exception(error_msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Function
# MAGIC
# MAGIC This section executes the revenue MDP processing workflow:
# MAGIC
# MAGIC 1. **Run Processing:**  
# MAGIC    Calls `process_psg_cert_revenue()` to generate the final revenue DataFrame.
# MAGIC
# MAGIC 2. **Write Output:**  
# MAGIC    Saves the result as a Delta table to the specified S3 path (`revenue_mdp_output`) in overwrite mode.
# MAGIC
# MAGIC 3. **Error Handling:**  
# MAGIC    - If writing fails, raises a descriptive exception.
# MAGIC    - If processing fails, raises a descriptive exception.

# COMMAND ----------

if __name__ == "__main__":
    try:
        df_revnue_mdp = process_psg_cert_revenue()
        try:
            df_revnue_mdp.write.mode("overwrite").format("delta").save(revenue_mdp_output)
        except Exception as write_err:
            raise Exception(f"Failed to write revenue_mdp to {revenue_mdp_output}: {str(write_err)}")
    except Exception as e:
        raise Exception(f"Failed to execute: {str(e)}")

