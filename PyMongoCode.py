# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Created By   **: NARESH** <br>
# MAGIC Created Date **: 07/22/2025** <br>
# MAGIC Purpose **     : This notebook extracts eligible reservations from Snowflake based on the rate codes and then create a reservation for players whom ever has not used the BKF yet.

# COMMAND ----------

# MAGIC %run /Library/SnowflakeModule

# COMMAND ----------

import json
import requests
import os
from datetime import datetime
from datetime import timedelta
from pytz import timezone
from pyspark.sql.functions import expr, explode, regexp_replace, regexp_extract, collect_set, collect_list, col, explode_outer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, ArrayType, StructType, MapType
from delta.tables import DeltaTable
from snowflake.snowpark import Row

# COMMAND ----------

dbutils.widgets.text("tablename", "BKFBUFFETRESERVATION","")
dbutils.widgets.text("JOB_RUN_ID", "-1","")
dbutils.widgets.text("PRCSSNG_RNGE_STRT_DT_TM","1900-12-31 00:00:00","PROCESSING RANGE START DATETIME")

table_name                =  getArgument("tablename").strip()
JOB_RUN_ID                =  getArgument("JOB_RUN_ID")
PRCSSNG_RNGE_STRT_DT_TM   =  getArgument("PRCSSNG_RNGE_STRT_DT_TM")

# Going 15 min back from the PRCSSNG_RNGE_STRT_DT_TM to get any missing data
start_tm_datetime = datetime.strptime(PRCSSNG_RNGE_STRT_DT_TM, "%Y-%m-%d %H:%M:%S")  
end_tm_datetime = start_tm_datetime - timedelta(minutes=15)  
PRCSSNG_RNGE_STRT_DT_TM = end_tm_datetime.strftime("%Y-%m-%d %H:%M:%S") 

print(f"table_name               :- {table_name}")
print(f"JOB_RUN_ID               :- {JOB_RUN_ID}")
print(f"PRCSSNG_RNGE_STRT_DT_TM  :- {PRCSSNG_RNGE_STRT_DT_TM}")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Setup SevenRooms Metadata

# COMMAND ----------

# MAGIC %run /cip/BKF/SevenRoomsModule

# COMMAND ----------

print(json.dumps(SR.endpoints, indent=4, sort_keys=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Set URL to BOOK reservation end point

# COMMAND ----------

venue_id = dbutils.secrets.get(f'cip-{azure_env}-shared-kv-scope', 'bkf-sevenrooms-venueId')
access_persistent_id = dbutils.secrets.get(f'cip-{azure_env}-shared-kv-scope', 'bkf-sevenrooms-accessPersistentId')

url_template= SR.endpoints['bookreservation']
url = url_template.replace("{venue_id}", venue_id)
token  = SR.sevenrooms_get_auth_token()
headers = {'Authorization' : token }

print(f"url :- {url}")
# print(f"headers :- {headers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set Snowflake values

# COMMAND ----------

if azure_env == 'prd':
  SFModule.warehouse = 'SEVENROOMS_WH'

## Setup Snowflake Schema
if azure_env == 'prd':
  SFModule.schema = 'EDW_PRD'
  EDW_SRC_DB_NAME = 'EDW_PRD'
if azure_env == 'stg':
  SFModule.schema = 'EDW_STG'
  EDW_SRC_DB_NAME = 'EDW_STG'
if azure_env in ['dev', 'tst']:
  SFModule.schema = 'EDW_DEV'
  EDW_SRC_DB_NAME = 'EDW_STG'

## Get Database Name
EDW_DB_NAME = SFModule.database

## Create Snowpark Session
sf_session = SFModule._SnowflakeCIPLib__get_sf_snowpark_session()  

# COMMAND ----------

# MAGIC %md
# MAGIC ###Format and set phone number, default email

# COMMAND ----------

# Condtional phone number logic to use whilte listed phone number in non-prod env
if EDW_DB_NAME == 'EDW_PRD':
  phone_number = "CONCAT('+', REGEXP_REPLACE(RHGP.PHONENUMBER, '[^0-9]',''))"
else:
  phone_number_qry = f"SELECT EDWVALUE FROM {EDW_DB_NAME}.CIPEDW.EDWVALUECONFIG WHERE EDWPROCESS='BKF' AND EDWPROCESSSUBTYPE='BKF Promo' AND EDWVALUETYPEID ='BKF-NONPROD Phone Number' AND RECORDACTIVEFLAG='Y'"
  phone_number_result = sf_session.sql(phone_number_qry).collect()
  phone_number = phone_number_result[0]['EDWVALUE']
  phone_number =  f"'{phone_number}'"

print(f"Phone Number: {phone_number}")

default_email_qry = f"SELECT EDWVALUE FROM {EDW_DB_NAME}.CIPEDW.EDWVALUECONFIG WHERE EDWPROCESS='BKF' AND EDWPROCESSSUBTYPE='BKF Promo' AND EDWVALUETYPEID ='BKF Default Email' AND RECORDACTIVEFLAG='Y'"
default_email_result = sf_session.sql(default_email_qry).collect()
default_email = default_email_result[0]['EDWVALUE']
default_email_local, default_email_domain = default_email.split("@",1)
default_email_domain = "@" + default_email_domain
print(f"Default Email Local: {default_email_local}")
print(f"Default Email Domain: {default_email_domain}")


# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 1: Update BKFBUFFETRESERVATION if any BKF reservations are complete and set active flag as N for old records

# COMMAND ----------

step1_qry = f"""
MERGE INTO {EDW_DB_NAME}.CIPEDW_OUTBOUND.BKFBUFFETRESERVATION AS TGT
USING
    (
    SELECT DININGRESERVATIONID, STATUSCODE 
    FROM {EDW_SRC_DB_NAME}.CIPEDW.SUMDININGRESERVATION
    WHERE BOOKEDBY='BKF'
    ) SRC
ON TGT.RESERVATIONID = SRC.DININGRESERVATIONID
WHEN MATCHED THEN
UPDATE SET 
TGT.BKF_RESERVATION_STATUS = SRC.STATUSCODE,
TGT.RECORDUPDATEDATETIME = CURRENT_TIMESTAMP(),
TGT.RECORDUPDATEUSERID = CURRENT_USER()
"""
result = sf_session.sql(step1_qry).collect()

step1_activeflg_qry =f"""
UPDATE {EDW_DB_NAME}.CIPEDW_OUTBOUND.BKFBUFFETRESERVATION
SET RECORDACTIVEFLAG='N',
RECORDUPDATEDATETIME = CURRENT_TIMESTAMP(),
RECORDUPDATEUSERID = CURRENT_USER()
WHERE BKFRESERVATIONDATE != CURRENT_DATE()
AND RECORDACTIVEFLAG='Y'
"""
result = sf_session.sql(step1_activeflg_qry).collect()

print("Step 1 completed - Update BKFBUFFETRESERVATION if any BKF reservations are complete and set active flag as N for old records")




# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2: Identify eligible reservations with eligible rate code for BKF Promo, that are not yet captured into BKF table

# COMMAND ----------

step2_qry = f"""
MERGE INTO {EDW_DB_NAME}.CIPEDW_OUTBOUND.BKFBUFFETRESERVATION TGT
USING
(
WITH
BKFTIME AS (SELECT EDWVALUE FROM {EDW_SRC_DB_NAME}.CIPEDW.EDWVALUECONFIG WHERE EDWPROCESS='BKF' AND EDWPROCESSSUBTYPE='BKF Promo' AND EDWVALUETYPEID ='BKF Reservation time' AND RECORDACTIVEFLAG='Y'),
BKFPRTYSIZE AS (SELECT EDWVALUE FROM {EDW_SRC_DB_NAME}.CIPEDW.EDWVALUECONFIG WHERE EDWPROCESS='BKF' AND EDWPROCESSSUBTYPE='BKF Promo' AND EDWVALUETYPEID ='BKF Party Size' AND RECORDACTIVEFLAG='Y'),
BKFRATECODE AS (SELECT EDWVALUE FROM {EDW_SRC_DB_NAME}.CIPEDW.EDWVALUECONFIG WHERE EDWPROCESS='BKF' AND EDWPROCESSSUBTYPE='BKF Promo' AND EDWVALUETYPEID ='BKF Rate Code' AND RECORDACTIVEFLAG='Y')
SELECT 
   DISTINCT
   SHR.GRIDNUM,
   SHR.PLAYERID,
   SHR.CONFIRMATIONNUMBER,
   SHR.RESERVATIONBEGINDATETIME AS CHECKINDATETIME,
   SHR.RESERVATIONENDDATETIME AS CHECKOUTDATETIME,
   RESERVATIONDATE AS BKFRESERVATIONDATE, 
   TIME(SELECT EDWVALUE FROM BKFTIME) AS BKFRESERVATIONTIME,
   (SELECT EDWVALUE FROM BKFPRTYSIZE) AS BKFPARTYSIZE,
   SHR.ROOMNUMBER,
   DHG.FIRSTNAME,
   DHG.LASTNAME,
   CASE 
        WHEN NULLIF(RHGE.EMAIL,'') IS NOT NULL THEN RHGE.EMAIL
        WHEN NULLIF(GRID_AGG.EMAIL,'') IS NOT NULL THEN GRID_AGG.EMAIL
        WHEN NULLIF(GRID_GUEST.EMAIL,'') IS NOT NULL THEN GRID_GUEST.EMAIL
        ELSE '{default_email_local}' || ROW_NUMBER() OVER (ORDER BY CONFIRMATIONNUMBER) || '{default_email_domain}'
   END AS EMAIL,
   CASE 
        WHEN NULLIF({phone_number},'') IS NOT NULL THEN {phone_number}
        WHEN NULLIF(GRID_AGG.PHONENUMBER,'') IS NOT NULL THEN CONCAT('+', REGEXP_REPLACE(GRID_AGG.PHONENUMBER, '[^0-9]',''))
        WHEN NULLIF(GRID_GUEST.PHONENUMBER,'') IS NOT NULL THEN CONCAT('+', REGEXP_REPLACE(GRID_GUEST.PHONENUMBER, '[^0-9]',''))
        ELSE NULL
   END AS PHONENUMBER, 
   SHR.RATECODE
FROM {EDW_SRC_DB_NAME}.USER_DB.VW_SUMHOTELGUESTDAILYAGG SHR
INNER JOIN {EDW_SRC_DB_NAME}.CIPEDW.DIMHOTELGUEST DHG
          ON SHR.GUESTNAMESOURCEID = DHG.GUESTNAMESOURCEID
          AND SHR.SOURCESYSTEMID = DHG.SOURCESYSTEMID
          AND DHG.SITECODE = 'WLV' AND DHG.RECORDACTIVEFLAG = 'Y'
INNER JOIN (SELECT EDWVALUE AS RATECODE FROM BKFRATECODE) CFG
          ON SHR.RATECODE ILIKE '%' || CFG.RATECODE || '%'          
LEFT JOIN {EDW_SRC_DB_NAME}.CIPEDW.REFHOTELGUESTEMAIL RHGE
          ON SHR.GUESTNAMESOURCEID = RHGE.GUESTNAMESOURCEID
          AND SHR.SOURCESYSTEMID = RHGE.SOURCESYSTEMID
          AND RHGE.SITECODE = 'WLV' AND RHGE.RECORDACTIVEFLAG = 'Y' AND RHGE.EMAILTYPE = 'EMAIL' AND RHGE.PRIMARYFLAG = 'Y'
LEFT JOIN {EDW_SRC_DB_NAME}.CIPEDW.REFHOTELGUESTPHONE RHGP
          ON SHR.GUESTNAMESOURCEID = RHGP.GUESTNAMESOURCEID
          AND SHR.SOURCESYSTEMID = RHGP.SOURCESYSTEMID
          AND RHGP.SITECODE = 'WLV' AND RHGP.RECORDACTIVEFLAG = 'Y' AND RHGP.PRIMARYFLAG = 'Y'
LEFT JOIN {EDW_SRC_DB_NAME}.CIPEDW.XREFGUESTGRIDNUM XREF
         ON SHR.GUESTNAMESOURCEID = TRY_CAST(XREF.GUEST_ID AS NUMERIC) 
         AND XREF.SOURCE_ID = 12 and XREF.FEED_ID = 101 AND XREF.RECORDACTIVEFLAG = 'Y'   
LEFT JOIN {EDW_SRC_DB_NAME}.CIPEDW.DIMGRIDGUEST GRID_GUEST
         ON XREF.GRIDNUM = GRID_GUEST.GRIDNUM AND GRID_GUEST.RECORDACTIVEFLAG='Y'
LEFT JOIN {EDW_SRC_DB_NAME}.CIPEDW.DIMGRIDGUEST GRID_AGG
         ON SHR.GRIDNUM = GRID_AGG.GRIDNUM AND GRID_AGG.RECORDACTIVEFLAG='Y'                             
WHERE SHR.SITECODE = 'WLV'
AND SHR.RESERVATIONSTATUS = 'CHECKED IN'
AND SHR.RESERVATIONDATE <> SHR.CHECKINDATE 
AND SHR.RESERVATIONDATE = CURRENT_DATE()
) SRC
ON COALESCE(TGT.GRIDNUM,-1)=COALESCE(SRC.GRIDNUM,-1)
AND COALESCE(TGT.CONFIRMATIONNUMBER,'')=COALESCE(SRC.CONFIRMATIONNUMBER,'')
AND COALESCE(TGT.BKFRESERVATIONDATE,'9999-12-31')=COALESCE(SRC.BKFRESERVATIONDATE,'9999-12-31')
WHEN NOT MATCHED THEN
INSERT 
(
GRIDNUM, PLAYERID, CONFIRMATIONNUMBER, CHECKINDATETIME, CHECKOUTDATETIME, BKFRESERVATIONDATE, BKFRESERVATIONTIME, BKFPARTYSIZE, ROOMNUMBER, FIRSTNAME, LASTNAME, EMAIL, PHONENUMBER, RATECODE, RECORDINSERTDATETIME, RECORDINSERTUSERID, RECORDACTIVEFLAG
)
VALUES
(
SRC.GRIDNUM, SRC.PLAYERID, SRC.CONFIRMATIONNUMBER, SRC.CHECKINDATETIME, SRC.CHECKOUTDATETIME, SRC.BKFRESERVATIONDATE, SRC.BKFRESERVATIONTIME, SRC.BKFPARTYSIZE, SRC.ROOMNUMBER, SRC.FIRSTNAME, SRC.LASTNAME, SRC.EMAIL, SRC.PHONENUMBER, SRC.RATECODE,  CURRENT_TIMESTAMP(), CURRENT_USER(), 'Y'
)
"""

result = sf_session.sql(step2_qry).collect()
print(f"Step 2 completed - Identify eligible reservations with eligible rate code for BKF Promo, that are not yet captured into BKF table - {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Request "create reservation 7R API End point" for below set of reservations only

# COMMAND ----------

step3_qry = f"""
SELECT GRIDNUM, PLAYERID, CONFIRMATIONNUMBER, BKFRESERVATIONDATE, BKFRESERVATIONTIME, BKFPARTYSIZE, FIRSTNAME, LASTNAME, PHONENUMBER, EMAIL
FROM {EDW_DB_NAME}.CIPEDW_OUTBOUND.BKFBUFFETRESERVATION BKF
WHERE 1=1
AND BKF.BKFRESERVATIONDATE = CURRENT_DATE()
AND COALESCE(BKF.CLIENTID,'') =''
"""

dfEligibileHotelReservations =  SFModule.get_snowpark_df(step3_qry)

print(f"Eligible Reservertions for BKF Promo: {dfEligibileHotelReservations.count()}")

api_results = []  # To store results for writing back

# Collect Snowpark rows to local memory
rowsEligibileHotelReservations = dfEligibileHotelReservations.collect()

# Loop through each row and send as API request
for idx, row in enumerate(rowsEligibileHotelReservations):
    payload = {
        "date": row['BKFRESERVATIONDATE'],
        "time": row['BKFRESERVATIONTIME'],
        "party_size": row['BKFPARTYSIZE'],
        "first_name": row['FIRSTNAME'],
        "last_name": row['LASTNAME'],
        "phone": row['PHONENUMBER'],
        "email": row['EMAIL'],
        "loyalty_id" : row['PLAYERID'],
        "notes" : row['CONFIRMATIONNUMBER'],
        'send_reminder_email': 'false',
        'send_reminder_sms':  'false',
        'send_client_email': 'false',
        'send_client_sms':  'false',        
        'access_persistent_id': access_persistent_id
    }

    try:

        response = requests.put(url, data=payload, headers=headers)
        result = response.json()
        if response.status_code == 200:
          # Handle successful response
          record = {
              "GRIDNUM": row['GRIDNUM'],
              "PLAYERID": row['PLAYERID'],
              "CONFIRMATIONNUMBER": row['CONFIRMATIONNUMBER'],
              "RESERVATION_ID": result.get("data", {}).get("reservation_id", ""),
              "RESERVATION_REFERENCE_CODE": result.get("data", {}).get("reservation_reference_code", ""),
              "CLIENT_ID": result.get("data", {}).get("client_id", ""),
              "CLIENT_REFERENCE_CODE": result.get("data", {}).get("client_reference_code", ""),
              "RESERVATION_CREATE_STATUS": "SUCCESS",
              "MSG": "",
              "REQUEST_ID": ""
          }
          api_results.append(record)
        else:
          # Error handling
          record = {
              "GRIDNUM": row['GRIDNUM'],
              "PLAYERID": row['PLAYERID'],
              "CONFIRMATIONNUMBER": row['CONFIRMATIONNUMBER'],
              "RESERVATION_ID": "",
              "RESERVATION_REFERENCE_CODE": "",
              "CLIENT_ID": "",
              "CLIENT_REFERENCE_CODE": "",
              "RESERVATION_CREATE_STATUS": "ERROR",
              "MSG": result.get("msg", ""),
              "REQUEST_ID": result.get("request_id", "")

          }
          api_results.append(record)
          
    except requests.exceptions.RequestException as e:
        # Catch hard failures only (e.g., timeout, DNS failure)
        print(str(e))    
        
print("Step 3 completed - Request to create reservation 7R API End point")
print(api_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Capture response into a temp table - TGT_STG_CIPEDW.STG_BKF7RAPIRESPONSE

# COMMAND ----------


# Convert list of dicts to Snowpark DataFrame
if api_results:
  response_rows = [Row(**r) for r in api_results]
  response_df = sf_session.create_dataframe(response_rows)

  ## BKF Response Snowflake Stage Table
  snf_stg_table = f"{EDW_DB_NAME}.TGT_STG_CIPEDW.STG_BKF7RAPIRESPONSE"

  ## Truncate the stage table
  sf_session.sql(f"USE SCHEMA {EDW_DB_NAME}.CIPEDW_OUTBOUND").collect(); sf_session.sql(f"TRUNCATE TABLE {snf_stg_table}").collect()

  ## Log Final Snowpark Dataframe into Snowflake
  SFModule.write_snowpark_df_to_sf(response_df, snf_stg_table, writemode="append")

print("Step 4 completed - Log response from 7R API into Snowflake")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5: Merge the API response to BKF Table - CIPEDW_OUTBOUND.BKFBUFFETRESERVATION

# COMMAND ----------

step5_qry = f"""
MERGE INTO {EDW_DB_NAME}.CIPEDW_OUTBOUND.BKFBUFFETRESERVATION AS TGT
USING
{EDW_DB_NAME}.TGT_STG_CIPEDW.STG_BKF7RAPIRESPONSE SRC
ON COALESCE(TGT.GRIDNUM,-1)=COALESCE(SRC.GRIDNUM,-1)
AND COALESCE(TGT.PLAYERID,-1)=COALESCE(SRC.PLAYERID,-1)
AND COALESCE(TGT.CONFIRMATIONNUMBER,'')=COALESCE(SRC.CONFIRMATIONNUMBER,'')
WHEN MATCHED THEN
UPDATE SET
TGT.RESERVATIONID = SRC.RESERVATION_ID,
TGT.REFERENCECODE = SRC.RESERVATION_REFERENCE_CODE,
TGT.CLIENTID = SRC.CLIENT_ID,
TGT.CLIENTREFERENCECODE = SRC.CLIENT_REFERENCE_CODE,
TGT.API_ERROR_MESSAGE = SRC.MSG,
TGT.API_ERROR_REQUEST_ID = SRC.REQUEST_ID,
TGT.BKF_RESERVATION_CREATE_DTTM = CURRENT_TIMESTAMP(),
TGT.BKF_RESERVATION_CREATE_STATUS = SRC.RESERVATION_CREATE_STATUS,
TGT.RECORDUPDATEDATETIME = CURRENT_TIMESTAMP(),
TGT.RECORDUPDATEUSERID = CURRENT_USER()
"""

sf_session.sql(step5_qry).collect()
print("Step 5 completed - Update BKF table with response from 7R API")
