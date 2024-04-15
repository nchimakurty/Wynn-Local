CREATE OR REPLACE PROCEDURE EDW_DEV.TGT_STG_CIPEDW.SP_INSERT_ROOM_RESERVATIONS("JSON_ARRAY" VARIANT)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('requests==2.31.0','snowflake==0.6.0', 'anyjson==0.3.3')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (MongoDB_GuestJourney_EXT_RULE)
SECRETS = ('API_TOKEN_SECRET'=EDW_DEV.TGT_STG_CIPEDW.GuestJourney_API_Token)
AS $$
import _snowflake
import requests
import json

def main(session, JSON_ARRAY): 
    api_base_url = "https://westus.azure.data.mongodb-api.com/app/data-mguxm/endpoint/data/v1"
    api_key = _snowflake.get_username_password('API_TOKEN_SECRET')
    data_source = "GuestJourneyOps-Dev"
    database_name = "GuestJourneyOps_Dev"
    collection_name = "ItineraryItem"  
    
    if JSON_ARRAY is None:
        return {"Error: Emprty JSON Array was passed"}

    documents = JSON_ARRAY
    result = upsert_document(api_key.password, api_base_url, data_source, database_name, collection_name, documents)
    return result


def upsert_document(api_key, api_base_url, data_source, database_name, collection_name, documents):
  endpoint = f"/action/updateMany"
  
  url = api_base_url + endpoint

  headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "apiKey": api_key
    }

  success_count = 0
  insert_count = 0
  fail_count = 0
  errors = []
  
  for doc in documents:
    payload = json.dumps({
      "dataSource": data_source,
      "database": database_name,
      "collection": collection_name,
      "filter": doc["filter"],
      "update": doc["update"],
      "upsert": True
    })

    
    response = requests.post(url, data=payload, headers=headers)

    if response.status_code in (200,201):
      # Successful request
      response_data = response.json()
      success_count += response_data.get('matchedCount', 0)
      insert_count += response_data.get('upsertedCount', 0)
    else:
      # Something went wrong
      fail_count +=1
      errors.append({"operation": doc, "error":response.text})

    final_status = {
        "success_count": success_count,
        "insert_count": insert_count,
        "fail_count": fail_count,
        "errors": errors
    }

    return(json.dumps(final_status))

$$;
