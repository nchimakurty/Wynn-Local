
# Databricks notebook source
# MAGIC %md
# MAGIC ## Library setup

# COMMAND ----------

pip install pymongo==4.3.2

# COMMAND ----------

# MAGIC %run /Library/SnowflakeModule

# COMMAND ----------

azure_env='dev'

# COMMAND ----------

if azure_env == 'dev':
  database_name = "Leaderboard_Dev"

if azure_env == 'tst':
  database_name = "Leaderboard_Test"

if azure_env == 'stg':
  database_name = "Leaderboard_Stg"

if azure_env == 'prd':
  database_name = "Leaderboard_Prd"

# COMMAND ----------

# MAGIC %run /cip/WynnRewards/2.0/WynnRewardsConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ## Libraries/setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import pymongo
import json
import datetime

# mongo_connection_string = dbutils.secrets.get(f'cip-{azure_env}-shared-kv-scope', 'cip-mongo-leaderboard-cs')
mongo_connection_string = 'mongodb+srv://svc_guestjourneyops_edw_dev_user:zhyL2KOBw7qoe0uy@guestjourneyops-dev-pri.xwl4k.mongodb.net/?retryWrites=true&w=majority&appName=GuestJourneyOps-Dev'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Function definitions

# COMMAND ----------

def get_mongo_client(database_uri=mongo_connection_string, database_name=database_name):
  #Function that returns an instance of a mongodb database with Pymongo
  client = pymongo.MongoClient(database_uri) # establishing connection to the database
  return client
   
def write_df_into_mongodb(df, target_collection, uri=mongo_connection_string, database=database_name, format='mongo', mode='overwrite', truncate='true'):
  #Function that writes one pyspark dataframe into a collection in mongodb with the spark-mongodb connector
  try:
    (
     df.write
       .format(format)
       .mode(mode)
       .option('truncate', truncate)
       .option("uri", uri)
       .option("database", database)
       .option("collection", target_collection)
       .save()
    ) 
  except Exception as err:
    raise Exception(f"Failed writing dataframe to mongo database: {database} - collection: {target_collection} with the error :- {str(err)}")  
        
def upsert_mongodb_collection(collection_target_name, collection_stg_name, unique_identifier_fields, mongo_connection_string=mongo_connection_string, database_name=database_name):
  #Function that upsert two collections inside of mongodb
  #A pipeline is set of operations that will be run inside mongodb, we use merge aggregation for the upsert (https://bit.ly/3zmKzcG)
  client = get_mongo_client()
  db = client[database_name]
  
  pipeline = [
    {
      "$merge": {
         "into": {
            "db": database_name,
            "coll": collection_target_name
            },
         "on": unique_identifier_fields,
         "whenMatched": "replace",
         "whenNotMatched": "insert"
      }
    }
  ]
  
  try:
    db[collection_stg_name].aggregate(pipeline) # this line executes the pipeline into mongodb
  except Exception as err:
    raise Exception(f"Failed merge into: {collection_target_name} with the error :- {str(err)}")  
    
def merge_df_to_mongo_collection(df_target_toMongo, target_collection, target_stg_collection):
  #Function that merges incoming dataframe to target collection in mongodb
  client = get_mongo_client()
  db = client[database_name]

  try:
    #### INSERT INTO MONGODB STG
    write_df_into_mongodb(df_target_toMongo, target_stg_collection)
    #### UPSERT STG COLLECTION INTO TARGET COLLECTION
    upsert_mongodb_collection(target_collection, target_stg_collection, ["_id"])
    #### DROP STG COLLECTION  
    db.drop_collection(target_stg_collection)    
         
  except Exception as err:
    #### DROP STG COLLECTION  
    db.drop_collection(target_stg_collection)
    raise Exception(f"Failed to merge data to mongo wynn rewards target collection: {target_collection} with the error :- {str(err)}")  
    
def delete_mongodb_documents(collection_target_name, df_data_to_delete, database_name=database_name):
  #Function to remove non active machines from the mongo db collection
  client = get_mongo_client()
  db = client[database_name]

  df = df_data_to_delete.toPandas()   
  # Convert the dataframe into dictionary 
  dict_data_to_delete = df.to_dict(orient = 'records') 

  try:
    for i in range(len(dict_data_to_delete)):
      db[collection_target_name].delete_many(dict_data_to_delete[i]) # this line remove the non activate machines from mongo collection
  except Exception as err:
    raise Exception(f"Failed delete many in collecction: {collection_target_name} with the error :- {str(err)}")  

# COMMAND ----------

def upsert_reservations_to_mongo(df, database_name, collection_name, mongo_connection_string, error_collection_name=None):
    """
    Upserts each row from the DataFrame into MongoDB.
    """
    import pymongo
    import json

    client = pymongo.MongoClient(mongo_connection_string)
    db = client[database_name]
    collection = db[collection_name]
    errors = []

    # Convert Spark DataFrame to dict records
    data = df.toPandas().to_dict(orient='records')

    for row in data:
        try:
            # Handle special_requests as list
            special_requests = row.get('SPECIAL_REQUESTS', [])
            if isinstance(special_requests, str):
                try:
                    special_requests = json.loads(special_requests)
                    if not isinstance(special_requests, list):
                        special_requests = []
                except Exception:
                    special_requests = []

            # Data type conversions as in the original code
            def to_int(val):
                try:
                    return int(val)
                except:
                    return None
            def to_float(val):
                try:
                    return float(val)
                except:
                    return None

            # Filter: Prefer CONFIRMATION_ID if present
            if row['CONFIRMATION_ID'] is not None:
                filter_doc = {
                    "player_id": row['PLAYER_ID'],
                    "confirmation_id": row['CONFIRMATION_ID']
                }
            else:
                filter_doc = {
                    "player_id": row['PLAYER_ID'],
                    "room.reservation_id": row['RESERVATION_ID']
                }
            
            # Build the update document, with conversions
            update_doc = {
                "$set": {
                    "start_date": row['START_DATE'],   # Already ISO string, can convert if needed
                    "end_date": row['END_DATE'],
                    "created_dtm": row['CREATED_DTM'],
                    "last_update_dtm": row['LAST_UPDATE_DTM'],
                    "site_id": to_int(row['SITE_ID']),
                    "status": row['STATUS'],
                    "last_name": row['LAST_NAME'],
                    "first_name": row['FIRST_NAME'],
                    "email": row['EMAIL'],
                    "phone_number": row['PHONE_NUMBER'],
                    "confirmation_id": row['CONFIRMATION_ID'],
                    "room": {
                        "reservation_id": row['RESERVATION_ID'],
                        "source_system_code": row['SOURCE_SYSTEM_CODE'],
                        "source_system_id": row['SOURCE_SYSTEM_ID'],
                        "room_type": row['ROOM_TYPE'],
                        "precheckin": row['PRECHECKIN'],
                        "special_requests": special_requests,
                        "occupants": to_int(row['OCCUPANTS']),
                        "adults": to_int(row['ADULTS']),
                        "children": to_int(row['CHILDREN']),
                        "market_code_first_day": row['MARKET_CODE_FIRST_DAY'],
                        "rate_code_first_day": row['RATE_CODE_FIRST_DAY'],
                        "block_code_first_day": row['BLOCK_CODE_FIRST_DAY'],
                        "deposit_amount": to_float(row['DEPOSIT_AMOUNT']),
                        "deposit_due_amount": to_float(row['DEPOSIT_DUE_AMOUNT']),
                        "guarantee_due": row['GUARANTEE_DUE'],
                        "company_booking": row['COMPANY_BOOKING'],
                        "routing_room": row['ROUTING_ROOM'],
                        "share_with_parent_id": row['SHARE_WITH_PARENT_ID']
                    }
                }
            }
            # Upsert into MongoDB
            collection.update_one(filter_doc, update_doc, upsert=True)
        
        except Exception as e:
            print(f"Error: {str(e)}") 
            error_detail = {
                "RESERVATION_ID": row.get('RESERVATION_ID', ''),
                "ERROR_MSG": str(e),
                "ROW": row
            }
            errors.append(error_detail)

    # Optionally log errors
    if error_collection_name and errors:
        db[error_collection_name].insert_many(errors)

    return {"total_processed": len(data), "errors": errors}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading reservations data from snowflake

# COMMAND ----------

SFModule.schema = 'CIPINTEGRATIONODS'

#reading game type data from snowflake
sql_query = f"""
SELECT DISTINCT
CAST(CAST(MBR.MEMBERSHIP_CARD_NO AS INTEGER) AS VARCHAR) AS PLAYER_ID,
CAST(CAST(RN.CONFIRMATION_NO AS INTEGER) AS VARCHAR) AS RESERVATION_ID,
EXRE.EXTERNAL_REFERENCE::VARCHAR AS CONFIRMATION_ID,
CONCAT(CAST(TO_DATE(RN.BEGIN_DATE) AS VARCHAR),'T00:00:00.000Z')  AS START_DATE,
CONCAT(CAST(TO_DATE(RN.END_DATE) AS VARCHAR),'T00:00:00.000Z')  AS END_DATE,
TO_CHAR(RN.INSERT_DATE, 'YYYY-MM-DDTHH:MI:SS.000Z') AS CREATED_DTM,
TO_CHAR(RN.UPDATE_DATE, 'YYYY-MM-DDTHH:MI:SS.000Z') AS LAST_UPDATE_DTM,
CASE WHEN RN.RESORT='WLV' THEN 1 WHEN RN.RESORT='BOS' THEN 2 ELSE 0 END AS SITE_ID,
'HOT' AS SOURCE_SYSTEM_CODE,
RDE.RESV_STATUS AS STATUS,
RN.PRECHECKIN::VARCHAR AS PRECHECKIN,
NM.LAST AS LAST_NAME,
NM.FIRST AS FIRST_NAME,
NMEM.PHONE_NUMBER AS EMAIL,
NMPH.PHONE_NUMBER AS PHONE_NUMBER,
CAST(CAST(NM.NAME_ID AS INTEGER) AS VARCHAR) AS SOURCE_SYSTEM_ID,
RDE.ROOM_LABEL AS ROOM_TYPE,
CAST(CAST((COALESCE(RDE.ADULTS, 0) + COALESCE(RDE.CHILDREN, 0)) AS INTEGER) AS VARCHAR) AS OCCUPANTS,
CAST(CAST(COALESCE(RDE.ADULTS,0) AS INTEGER) AS VARCHAR) AS ADULTS,
CAST(CAST(COALESCE(RDE.CHILDREN,0) AS INTEGER) AS VARCHAR) AS CHILDREN,
RDE.MARKET_CODE::VARCHAR AS MARKET_CODE_FIRST_DAY,
RDE.RATE_CODE::VARCHAR AS RATE_CODE_FIRST_DAY,
RDE.BLOCKCODE::VARCHAR AS BLOCK_CODE_FIRST_DAY,
CAST(ROUND(COALESCE(CA.DEPOSIT_AMOUNT,0),2) AS VARCHAR) AS DEPOSIT_AMOUNT,
CAST(ROUND((COALESCE(CA.DEPOSIT_AMOUNT,0) - COALESCE(CP.DEPOSIT_PAID,0)), 2) AS VARCHAR) AS DEPOSIT_DUE_AMOUNT,
RDE.GUARANTEE_CODE::VARCHAR AS GUARANTEE_DUE,
RDE.COMPANYBOOKING::VARCHAR AS COMPANY_BOOKING,
RDE.ROUTINGROOM::VARCHAR AS ROUTING_ROOM,
'' AS SHARE_WITH_PARENT_ID,
SR.SPECIAL_REQUESTS AS SPECIAL_REQUESTS,
RN.QLK_LOAD_TIMESTAMP
FROM 
--EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_RESV_DLT_WLV RN -- Orig view
EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_RESV_DLT_TEST_WLV RN -- Test only 
JOIN EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_ACTIVE_MEMBERSHIPS_WLV MBR ON RN.NAME_ID = MBR.NAME_ID
LEFT JOIN EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_EXTERNAL_REFERENCES_WLV EXRE ON RN.RESV_NAME_ID = EXRE.RESV_NAME_ID
LEFT JOIN EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_NAME_WLV NM ON MBR.NAME_ID = NM.NAME_ID
LEFT JOIN EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_EMAIL_WLV NMEM ON MBR.NAME_ID = NMEM.NAME_ID
LEFT JOIN EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_PHONE_WLV NMPH ON MBR.NAME_ID = NMPH.NAME_ID
LEFT JOIN EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_RESV_DATA_FIELDS_WLV RDE ON RN.RESV_NAME_ID = RDE.RESV_NAME_ID
LEFT JOIN EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_DEPOSIT_AMOUNT_WLV CA ON RN.RESV_NAME_ID = CA.RESV_NAME_ID
LEFT JOIN EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_DEPOSIT_PAID_WLV CP ON RN.RESV_NAME_ID = CP.RESV_NAME_ID
LEFT JOIN EDW_{env}.CIPINTEGRATIONODS.VW_CX_IB_RESERVATION_SPECIAL_REQUESTS_WLV SR ON RN.RESV_NAME_ID = SR.RESV_NAME_ID
"""

df = SFModule.get_spark_df_from_sf(sqlQuery=sql_query)


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Merge dataframes to mongo

# COMMAND ----------

try:
  result = upsert_reservations_to_mongo(
      df=df,
      database_name='GuestJourneyOps_Dev',
      collection_name='ItineraryItem',
      mongo_connection_string=mongo_connection_string,
      #error_collection_name="UpsertErrors"      # Optional: saves error rows in a MongoDB collection
  )

  print("Done! Processed:", result["total_processed"], "Errors:", len(result["errors"]))

  

except Exception as err:
  raise Exception(f"Failed to transform dataframes with the error :- {str(err)}")    

# COMMAND ----------

dbutils.notebook.exit(json.dumps({'jobStatus' : 'C' }))   
