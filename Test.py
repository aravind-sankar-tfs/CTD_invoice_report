# Databricks notebook source
from pyspark.sql import functions as F

# Load base tables
docline = spark.table("psg_mydata_production_euw1_raw_erp.coda_oas_docline")
dochead = spark.table("psg_mydata_production_euw1_raw_erp.coda_oas_dochead")
company = spark.table("psg_mydata_production_euw1_raw_erp.coda_oas_company")
el3_element = spark.table("psg_mydata_production_euw1_raw_erp.coda_oas_el3_element")
coda_ref_corr = spark.table("CODA-Reference Correction")

# AggregatedDL
aggdl = (
    docline
    .filter(F.col("el2").like("021%"))
    .groupBy("cmpcode", "doccode", "docnum", "statpay")
    .agg(
        F.sum(
            F.when(F.col("valuedoc_dp").isNull() | (F.col("valuedoc_dp") == 0), F.lit(0))
            .otherwise(F.col("valuedoc") / F.pow(10, F.col("valuedoc_dp") - 2))
        ).alias("valuedoc"),
        F.sum(
            F.when(F.col("valuehome_dp").isNull() | (F.col("valuehome_dp") == 0), F.lit(0))
            .otherwise(F.col("valuehome") / F.pow(10, F.col("valuehome_dp") - 2))
        ).alias("valuehome"),
        F.max("el2").alias("el2"),
        F.max("el3").alias("el3"),
        F.max("ref1").alias("ref1"),
        F.max("ref2").alias("ref2"),
        F.max("ref3").alias("ref3"),
        F.max("ref4").alias("ref4"),
        F.max("ref5").alias("ref5"),
    )
)

# AggregatedDL2
aggdl2 = (
    docline
    .filter((~F.col("el2").like("021%")) & (F.col("el4") != ""))
    .groupBy("cmpcode", "doccode", "docnum")
    .agg(F.max("el4").alias("el4"))
)

# FinalAggregated
dh = dochead.alias("DH")
dl = aggdl.alias("DL")
dl2 = aggdl2.alias("DL2")
c = company.alias("C")
cus = el3_element.alias("CUS")

siteid_map = {
    'ALLENTOWN': '0', 'BAS': '1', 'MAI': '2', 'MP': '4', 'WAR': '13', 'CLINTRAK': '99',
    'INDY': '20', 'KOREA': '33', 'JAPAN': '15', 'SINGAPORE': '3', 'CHINA': '7',
    'SUZHOU': '31', 'HEG': '38'
}

def siteid_expr(col):
    return F.coalesce(
        F.create_map([F.lit(k), F.lit(v) for k, v in siteid_map.items()])[col],
        F.lit("Unknown")
    )

final_agg = (
    dh
    .join(dl, [F.trim(dl.cmpcode) == F.trim(dh.cmpcode),
               F.trim(dl.doccode) == F.trim(dh.doccode),
               F.trim(dl.docnum) == F.trim(dh.docnum)], "left")
    .join(dl2, [F.trim(dl2.cmpcode) == F.trim(dl.cmpcode),
                F.trim(dl2.doccode) == F.trim(dl.doccode),
                F.trim(dl2.docnum) == F.trim(dl.docnum)], "left")
    .join(c, c.code == dl.cmpcode, "left")
    .join(cus, (cus.el3_cmpcode == dl.cmpcode) & (cus.el3_code == dl.el3) & (cus.el3_elmlevel == 3), "left")
    .filter(dh.cmpcode.isin(list(siteid_map.keys())))
    .filter(dl.statpay != 665)
    .filter(dh.docdate >= F.lit("2019-01-01"))
    .filter(~dh.doccode.isin(
        'DISPERSE', 'MATCHING', 'PCANCEL', 'PINV', 'PINVDBT', 'RECEIPTS', 'Y/E-PROC-BS', 'CPAY',
        'CREC', 'OBAL', 'PCRN', 'REVAL', 'REVALR', 'YE-PROC-BS', 'PINV_XL', 'PINVDBT_XL',
        'PINV2', 'PINV1', 'ACCLTBI', 'ACCLTBIREV', 'ACCREV', 'ACCRUAL', 'CORRECTIVE', 'PCRNIC',
        'PINVIC', 'RECLASS', 'REVACC', 'REVERSAL', 'JGEN', 'JGENREV', 'JREVGEN'
    ))
    .select(
        siteid_expr(dh.cmpcode).alias("SiteID"),
        dh.cmpcode,
        dh.doccode,
        dh.docdate.alias("InvoiceDate"),
        F.when(
            dh.cmpcode.isin('ALLENTOWN', 'MAI', 'CHINA', 'SINGAPORE', 'CLINTRAK', 'MP'), dl.ref1
        ).when(
            dh.cmpcode.isin('JAPAN', 'SUZHOU'), dl.ref5
        ).otherwise(F.trim(dh.docnum)).alias("InvoiceNumber"),
        F.when(dh.doccode.like("%CR%"), "Credit Note").otherwise("Invoice").alias("DocType"),
        F.upper(
            F.when((dh.cmpcode == "BAS") & (dl.ref5.like("PT%")), F.substring(dl.ref5, 1, 6))
            .when((dh.cmpcode == "BAS") & (~dl.ref5.like("PT%")), dh.cmpcode)
            .when((dh.cmpcode == "WAR") & (dl.ref5.like("PT%")), F.substring(dl.ref5, 1, 6))
            .when((dh.cmpcode == "WAR") & (~dl.ref5.like("PT%")), dh.cmpcode)
            .when((dh.cmpcode == "ALLENTOWN") & (dl2.el4.like("X%")), dl2.el4)
            .when((dh.cmpcode == "ALLENTOWN") & (~dl2.el4.like("X%")), dh.cmpcode)
            .when((dh.cmpcode == "MP") & (dl2.el4.like("X%")), dl2.el4)
            .when((dh.cmpcode == "MP") & (~dl2.el4.like("X%")), dh.cmpcode)
            .when((dh.cmpcode == "CLINTRAK") & (dl2.el4.like("X%")), dl2.el4)
            .when((dh.cmpcode == "CLINTRAK") & (~dl2.el4.like("X%")), dh.cmpcode)
            .when((dh.cmpcode == "MAI") & (dl.ref3.like("PT%")), F.substring(dl.ref3, 1, 6))
            .when((dh.cmpcode == "MAI") & (~dl.ref3.like("PT%")), dh.cmpcode)
            .otherwise("Unknown")
        ).alias("ServiceDeliverySite"),
        F.when((dh.cmpcode == "BAS") & (dl.ref5.like("PT%")), F.substring(dl.ref5, 8, 100))
        .when((dh.cmpcode == "BAS") & (~dl.ref5.like("PT%")), F.trim(dh.docnum))
        .when((dh.cmpcode == "WAR") & (dl.ref5.like("PT%")), F.substring(dl.ref5, 8, 100))
        .when((dh.cmpcode == "WAR") & (~dl.ref5.like("PT%")), F.trim(dh.docnum))
        .when((dh.cmpcode == "ALLENTOWN") & (dl2.el4.like("X%")), dl.ref5)
        .when((dh.cmpcode == "ALLENTOWN") & (~dl2.el4.like("X%")), dl.ref1)
        .when((dh.cmpcode == "CLINTRAK") & (dl2.el4.like("X%")), dl.ref5)
        .when((dh.cmpcode == "CLINTRAK") & (~dl2.el4.like("X%")), dl.ref1)
        .when((dh.cmpcode == "MP") & (dl2.el4.like("X%")), dl.ref5)
        .when((dh.cmpcode == "MP") & (~dl2.el4.like("X%")), dl.ref1)
        .when((dh.cmpcode == "MAI") & (dl.ref3.like("PT%")), F.substring(dl.ref3, 8, 100))
        .when((dh.cmpcode == "MAI") & (~dl.ref3.like("PT%")), dl.ref1)
        .otherwise("Unknown").alias("ServiceDeliverySiteInvoiceNumber"),
        F.when((dh.cmpcode == "BAS") & (dl.el3.like("ICOM%")), "IC")
        .when((dh.cmpcode == "BAS") & (dl.ref5.like("PT%")), "3PD-PT")
        .when((dh.cmpcode == "WAR") & (dl.el3.like("ICOM%")), "IC")
        .when((dh.cmpcode == "WAR") & (dl.ref5.like("PT%")), "3PD-PT")
        .when((dh.cmpcode == "ALLENTOWN") & (dl.el3.like("X%")), "IC")
        .when((dh.cmpcode == "ALLENTOWN") & (dl2.el4.like("X%")), "3PD-PT")
        .when((dh.cmpcode == "CLINTRAK") & (dl.el3.like("X%")), "IC")
        .when((dh.cmpcode == "CLINTRAK") & (dl2.el4.like("X%")), "3PD-PT")
        .when((dh.cmpcode == "MP") & (dl.el3.like("X%")), "IC")
        .when((dh.cmpcode == "MP") & (dl2.el4.like("X%")), "3PD-PT")
        .when((dh.cmpcode == "MAI") & (dl.el3.like("IF%")), "IC")
        .when((dh.cmpcode == "MAI") & (dl.ref3.like("PT%")), "3PD-PT")
        .otherwise("3PD-OWN").alias("InvoiceType"),
        F.upper(
            dh.cmpcode +
            F.when((dh.cmpcode == "BAS") & (dl.ref5.like("PT%")), F.substring(dl.ref5, 1, 6))
            .when((dh.cmpcode == "BAS") & (~dl.ref5.like("PT%")), dh.cmpcode)
            .when((dh.cmpcode == "WAR") & (dl.ref5.like("PT%")), F.substring(dl.ref5, 1, 6))
            .when((dh.cmpcode == "WAR") & (~dl.ref5.like("PT%")), dh.cmpcode)
            .when((dh.cmpcode == "ALLENTOWN") & (dl2.el4.like("X%")), dl2.el4)
            .when((dh.cmpcode == "ALLENTOWN") & (~dl2.el4.like("X%")), dh.cmpcode)
            .when((dh.cmpcode == "MP") & (dl2.el4.like("X%")), dl2.el4)
            .when((dh.cmpcode == "MP") & (~dl2.el4.like("X%")), dh.cmpcode)
            .when((dh.cmpcode == "CLINTRAK") & (dl2.el4.like("X%")), dl2.el4)
            .when((dh.cmpcode == "CLINTRAK") & (~dl2.el4.like("X%")), dh.cmpcode)
            .when((dh.cmpcode == "MAI") & (dl.ref3.like("PT%")), F.substring(dl.ref3, 1, 6))
            .when((dh.cmpcode == "MAI") & (~dl.ref3.like("PT%")), dh.cmpcode)
            .otherwise("Unknown")
        ).alias("ICEntityMapKey"),
        (siteid_expr(dh.cmpcode) + F.lit("-") + dl.el3).alias("_keyClientID"),
        dl.el3.alias("CustomerNumber"),
        cus.el3_name.alias("CustomerName"),
        F.when(dh.cmpcode == "CHINA", dl.ref2)
        .when(dh.cmpcode == "WAR", dl.ref3)
        .otherwise(dl.ref4).alias("PO"),
        F.when(dh.cmpcode.isin("JAPAN", "SUZHOU"), dl.ref3)
        .otherwise(dl.ref2).alias("Protocol"),
        F.when((dh.cmpcode == "BAS") & (dl.ref5.like("PT%")), dl.ref3)
        .otherwise(F.coalesce(dl2.el4, F.lit(""))).alias("JobNumber"),
        dl.statpay,
        dl.valuehome,
        dl.valuedoc
    )
    .filter(
        ~F.coalesce(F.upper(F.col("ServiceDeliverySiteInvoiceNumber")), F.lit("[]")).like("%REIMB%")
    )
    .filter(
        ~F.coalesce(F.lower(F.col("ServiceDeliverySiteInvoiceNumber")), F.lit("[]")).like("%olidated%")
    )
)

# Split InvoiceDate to date only
final_agg = final_agg.withColumn("InvoiceDate", F.to_date(F.col("InvoiceDate")))

# Add InterimKey
final_agg = final_agg.withColumn(
    "InterimKey",
    F.concat_ws("-", F.col("cmpcode"), F.col("doccode"), F.trim(F.col("InvoiceNumber")), F.col("ServiceDeliverySiteInvoiceNumber"))
)

# Join CODA-Reference Correction for Corrected-Originating Invoice
final_agg = final_agg.join(
    coda_ref_corr.select(F.col("InterimKey").alias("corr_key"), F.col("FieldValue").alias("Corrected-Originating Invoice")),
    final_agg.InterimKey == F.col("corr_key"),
    "left"
).drop("corr_key")

# Add InterimKey-SVCDVSite
final_agg = final_agg.withColumn(
    "InterimKey-SVCDVSite",
    F.concat_ws("-", F.col("cmpcode"), F.col("doccode"), F.trim(F.col("InvoiceNumber")), F.col("ServiceDeliverySite"))
)

# Join CODA-Reference Correction for Corrected-OriginatingSite
final_agg = final_agg.join(
    coda_ref_corr.select(F.col("InterimKey").alias("corr_key2"), F.col("FieldValue").alias("Corrected-OriginatingSite")),
    final_agg["InterimKey-SVCDVSite"] == F.col("corr_key2"),
    "left"
).drop("corr_key2")

# Add New ServiceDeliverySite
final_agg = final_agg.withColumn(
    "New ServiceDeliverySite",
    F.when(F.col("Corrected-OriginatingSite").isNull(), F.col("ServiceDeliverySite")).otherwise(F.col("Corrected-OriginatingSite"))
)

# Add New ServiceDeliverySiteInvoiceNumber
final_agg = final_agg.withColumn(
    "New ServiceDeliverySiteInvoiceNumber",
    F.when(
        F.col("Corrected-Originating Invoice").isNull() & (F.col("ServiceDeliverySiteInvoiceNumber") == ""),
        F.col("InvoiceNumber")
    ).when(
        F.col("Corrected-Originating Invoice").isNull(),
        F.col("ServiceDeliverySiteInvoiceNumber")
    ).otherwise(F.col("Corrected-Originating Invoice"))
)

# Remove columns and rename
cols_to_drop = [
    "ServiceDeliverySite", "ServiceDeliverySiteInvoiceNumber",
    "Corrected-Originating Invoice", "Corrected-OriginatingSite",
    "InterimKey", "InterimKey-SVCDVSite"
]
final_agg = final_agg.drop(*cols_to_drop)
final_agg = final_agg.withColumnRenamed("New ServiceDeliverySite", "ServiceDeliverySite") \
                     .withColumnRenamed("New ServiceDeliverySiteInvoiceNumber", "ServiceDeliverySiteInvoiceNumber")

# Add _InvoiceSite-DeliverySite Key
final_agg = final_agg.withColumn("_InvoiceSite-DeliverySite", F.concat(F.col("cmpcode"), F.col("ServiceDeliverySite")))

display(final_agg)

# COMMAND ----------

jdbc_url = (
    "jdbc:sqlserver://fvzh3nukvj3upilj5pvxu2r3m4-dhobibgnitienogvfhocprazui.datawarehouse.fabric.microsoft.com:1433;"
    "databaseName=DW_Platinum_Prod"
)

connection_properties = {
    "user": "raheem.shaik@thermofisher.com",
    "password": "xxxxxxx",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "authentication": "ActiveDirectoryPassword",
}

table_name = "[dbo].[Finance - CDS - Billing]"

df = spark.read.jdbc(
    url=jdbc_url,
    table=table_name,
    properties=connection_properties
)
display(df)

# COMMAND ----------

import logging
import msal

def build_url(driveId, path, fileName):
    prefix = "https://graph.microsoft.com/v1.0/drives/"
    return f"{prefix}{driveId}{path}{fileName}:/content"

def get_token(secret, clientId):
    authority_url = "https://login.microsoftonline.com/b67d722d-aa8a-4777-a169-ebeb7a6a3b67"
    app = msal.ConfidentialClientApplication(
        client_id=clientId,
        client_credential=secret,
        authority=authority_url
    )
    token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    return token

logger = logging.getLogger(__name__)

def get_sharepoint_excel_df(weburl, file, client_id, client_secret, driver_id, skip_num_rows, sheet_prefix="Month"):
    try:
        # Get access token
        token = get_token(client_secret, client_id)["access_token"]

        # Build download URL
        download_url = build_url(driveId=driver_id, path=weburl, fileName=file)
        response = requests.get(
            download_url,
            allow_redirects=True,
            headers={'Authorization': f'Bearer {token}'}
        )

        if response.status_code != 200:
            logger.error(f"[{response.status_code}] Failed to download file from SharePoint: {download_url}")
            logger.info("SharePoint site may be inaccessible. We are working to resolve it and will keep you updated.")
            raise ValueError(f"Failed to download file: {response.status_code}")

        content_type = response.headers.get('Content-Type', '')
        file_extension = file.lower().split('.')[-1]

        # Check based on file extension and content-type
        if file_extension == 'xlsx':
            if 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' not in content_type:
                logger.error(f"Input file '{file}' is unavailable in .xlsx format. Detected content type: {content_type}")
                raise ValueError(f"Unexpected content type for .xlsx file: {content_type}")
            engine = 'openpyxl'
        elif file_extension == 'xlsb':
            if 'application/vnd.ms-excel.sheet.binary.macroEnabled.12' not in content_type:
                logger.error(f"Input file '{file}' is unavailable in .xlsb format. Detected content type: {content_type}")
                raise ValueError(f"Unexpected content type for .xlsb file: {content_type}")
            engine = 'pyxlsb'
        else:
            raise ValueError("Unsupported file format. Only .xlsx and .xlsb are supported.")

        # Load Excel file
        excel_file = pd.ExcelFile(io.BytesIO(response.content), engine=engine)
        sheet_name = next((s for s in excel_file.sheet_names if s.startswith(sheet_prefix)), None)

        if not sheet_name:
            logger.error(f"No sheet starting with '{sheet_prefix}' found in the file '{file}'.")
            raise ValueError(f"No sheet starting with '{sheet_prefix}' found in the file.")

        # Read sheet
        df = pd.read_excel(excel_file, sheet_name=sheet_name, skiprows=skip_num_rows, engine=engine)

        return df

    except Exception as e:
        logger.exception(
            f"Input file '{file}' is unavailable in the SharePoint site or cannot be read. Please check from your side and share the input files."
        )
        raise

def get_sharepoint(weburl, file, client_id, client_secret, driver_id, s3_bucket="s3://tfsdl-lslpg-fdt-test"):
    try:
        api_key = get_token(client_secret, client_id)["access_token"]
        download_url = build_url(driveId=driver_id, path=weburl, fileName=file)
        readSharepoint = requests.get(
            download_url,
            allow_redirects=True,
            headers={'Authorization': f'Bearer {api_key}'}
        )

        # Save locally first
        local_path = f"/tmp/{file.replace(' ', '_')}"
        with open(local_path, "wb") as fptr:
            fptr.write(readSharepoint.content)

        # Upload to S3
        s3_path = f"{s3_bucket}/{file.replace(' ', '_').replace('.xlsx','')}"
        dbutils.fs.cp(f"file://{local_path}", s3_path)
        print(s3_path)
    except FileNotFoundError as e:
        logger.warning(f"{e} Download failed")
        return None
    else:
        if readSharepoint.status_code == 200:
            logger.info(f"HTTP {readSharepoint.status_code}: File successfully downloaded from SharePoint and uploaded to S3")
            return s3_path  # S3 path returned
        else:
            logger.warning(f"HTTP {readSharepoint.status_code}: Unable to download file from SharePoint")
            return None

# COMMAND ----------

import requests
import io
import pandas as pd

temp = "s3a://tfsdl-lsg-spot-test/databricks/data"
fids_all_user = 'FIN_DIGI_TF_DEV_ETL_USER'
fids_all_password = dbutils.secrets.get('ds-crg-fdt', 'fin_dev')
fids_all_url = (
    "jdbc:snowflake://ppd.us-east-1.snowflakecomputing.com:443/EDWD"
    f"?user={fids_all_user}&password={fids_all_password}"
)

# Use secrets DBUtil to get Snowflake credentials.
client_id = 'ffdd77e0-b8f0-4c02-879e-51c41bbcd0fa'
client_secret = dbutils.secrets.get('ds-lsg-fids', 'fids_azure_sharepoint_app_secret')
driver_id = dbutils.secrets.get('ds-lsg-fids', 'fids_sharepoint_driverID') + '/root:'  # sharepoint driver id plus
token = get_token(client_secret, client_id)["access_token"]
weburl = "/CTD/"
# weburl = "/CRG/DS%20Internship%20Program%202025/PPD%20Unit%20Grid%20Mapping/data/"

# COMMAND ----------

file_name ="CODA-Reference Correction.xlsx"
coda_ref_correction = get_sharepoint_excel_df(
            weburl + '/', file_name, client_id, client_secret, driver_id,
            skip_num_rows=0, sheet_prefix="Sheet1")

# COMMAND ----------

display(coda_ref_correction)
