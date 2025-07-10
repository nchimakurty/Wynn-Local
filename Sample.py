import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, listagg, trim
from snowflake.snowpark import Row
import _snowflake
import requests
import json
import io


def main(session: snowpark.Session):

    api_config = _snowflake.get_generic_secret_string('API_CONFIG')
    api_config_dict = json.loads(api_config)

    api_key = api_config_dict['api_key']
    api_base_url = api_config_dict['api_base_url']
    data_source = api_config_dict['data_source']
    database_name = api_config_dict['database_name']
    collection_name = api_config_dict['collection_name']
    mongodb_endpoint_url = api_base_url + f"/action/updateMany"

    mongodb_endpoint_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "apiKey": api_key
        }

    clear_log_query = "DELETE FROM {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_LOG_TBL WHERE SP_NAME = 'SP_CX_IB_WLV_HOTEL_RESV_LOAD'"
    session.sql(clear_log_query).collect()
    
    try:

        getLastBatchStatusSql = "SELECT MAX(BATCH_ID) MAX_BATCH_ID FROM {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_HOTEL_RESV_LOAD_STATUS WHERE BATCH_STATUS='R' AND SITE_ID='WLV'"
        lastBatchId = session.sql(getLastBatchStatusSql).collect()
        lastBatchIdVal = lastBatchId[0]['MAX_BATCH_ID']
        
        if lastBatchIdVal is not None:
            updateLastBatchStatusSql = "UPDATE {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_HOTEL_RESV_LOAD_STATUS SET BATCH_STATUS='F' WHERE BATCH_STATUS='R' AND SITE_ID='WLV'"
            session.sql(updateLastBatchStatusSql).collect()
        
        clearBatchStatusSql = "DELETE FROM {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_HOTEL_RESV_LOAD_STATUS WHERE SITE_ID='WLV' AND BATCH_END_TS < CURRENT_DATE()- INTERVAL '7 days'"
        session.sql(clearBatchStatusSql).collect()

        deleteLastBatchLoadSql = f"TRUNCATE TABLE {{EDW_DB_NAME}}.CIPINTEGRATIONODS.STG_CX_IB_WLV_HOTEL_RESV_LOAD"
        session.sql(deleteLastBatchLoadSql).collect()

        log_msg(f'Last batch data deleted', session)

        createNewBatchSql = f"""INSERT INTO {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_HOTEL_RESV_LOAD_STATUS (SITE_ID, BATCH_ID, BATCH_STATUS, EXTRACT_FROM_TS, EXTRACT_TO_TS, EXTRACT_COUNT, BATCH_START_TS, BATCH_END_TS)
                                (SELECT 'WLV', TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISS'), 'R', (SELECT COALESCE(MAX(EXTRACT_TO_TS),(CURRENT_TIMESTAMP() - INTERVAL '1 minute')) EXTARCT_TO_TS 
                                FROM {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_HOTEL_RESV_LOAD_STATUS WHERE BATCH_STATUS='C' AND SITE_ID='WLV'), NULL, NULL, CURRENT_TIMESTAMP(), NULL)"""

        session.sql(createNewBatchSql).collect()

        log_msg(f'New batch started', session)

        loadRoomReservationsSql = f"""
        INSERT INTO {{EDW_DB_NAME}}.CIPINTEGRATIONODS.STG_CX_IB_WLV_HOTEL_RESV_LOAD
        (PLAYER_ID, RESERVATION_ID, CONFIRMATION_ID, START_DATE, END_DATE, CREATED_DTM, LAST_UPDATE_DTM, SITE_ID, SOURCE_SYSTEM_CODE, STATUS, PRECHECKIN, LAST_NAME, FIRST_NAME, EMAIL, 
         PHONE_NUMBER, SOURCE_SYSTEM_ID, ROOM_TYPE, OCCUPANTS, ADULTS, CHILDREN, MARKET_CODE_FIRST_DAY, RATE_CODE_FIRST_DAY, BLOCK_CODE_FIRST_DAY, DEPOSIT_AMOUNT, DEPOSIT_DUE_AMOUNT, GUARANTEE_DUE,
         COMPANY_BOOKING, ROUTING_ROOM, SHARE_WITH_PARENT_ID, QLK_LOAD_TIMESTAMP)
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
        {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_RESV_DLT_WLV RN -- Orig view
        --{{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_RESV_DLT_TEST_WLV RN -- Test only 
        JOIN {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_ACTIVE_MEMBERSHIPS_WLV MBR ON RN.NAME_ID = MBR.NAME_ID
        LEFT JOIN {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_EXTERNAL_REFERENCES_WLV EXRE ON RN.RESV_NAME_ID = EXRE.RESV_NAME_ID
        LEFT JOIN {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_NAME_WLV NM ON MBR.NAME_ID = NM.NAME_ID
        LEFT JOIN {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_EMAIL_WLV NMEM ON MBR.NAME_ID = NMEM.NAME_ID
        LEFT JOIN {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_PHONE_WLV NMPH ON MBR.NAME_ID = NMPH.NAME_ID
        LEFT JOIN {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_RESV_DATA_FIELDS_WLV RDE ON RN.RESV_NAME_ID = RDE.RESV_NAME_ID
        LEFT JOIN {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_DEPOSIT_AMOUNT_WLV CA ON RN.RESV_NAME_ID = CA.RESV_NAME_ID
        LEFT JOIN {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_DEPOSIT_PAID_WLV CP ON RN.RESV_NAME_ID = CP.RESV_NAME_ID
        LEFT JOIN {{EDW_DB_NAME}}.CIPINTEGRATIONODS.VW_CX_IB_RESERVATION_SPECIAL_REQUESTS_WLV SR ON RN.RESV_NAME_ID = SR.RESV_NAME_ID
        """

        session.sql(loadRoomReservationsSql).collect()

        insert_log_query = f"INSERT INTO {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_LOG_TBL VALUES ('SP_CX_IB_WLV_HOTEL_RESV_LOAD', CURRENT_TIMESTAMP(),'Room reservations stage load completed')"
        session.sql(insert_log_query).collect()

        getMaxUpdtTsSql = f"""SELECT COALESCE(MAX(LAST_UPDATE_DTM),TO_CHAR(CURRENT_TIMESTAMP(),'YYYY-MM-DDTHH24:MI:SS.000Z')) AS MAX_LAST_UPDATE_DTM 
                              FROM {{EDW_DB_NAME}}.CIPINTEGRATIONODS.STG_CX_IB_WLV_HOTEL_RESV_LOAD"""
        getMaxUpdtTsSqlRslt = session.sql(getMaxUpdtTsSql).collect()
        maxUpdtTs = getMaxUpdtTsSqlRslt[0]['MAX_LAST_UPDATE_DTM']

        getRoomReservationsCntSql = f"SELECT COUNT(1) AS RESV_CNT FROM {{EDW_DB_NAME}}.CIPINTEGRATIONODS.STG_CX_IB_WLV_HOTEL_RESV_LOAD"
        getRoomReservationsCntSqlRslt = session.sql(getRoomReservationsCntSql).collect()
        roomReservationsCnt = getRoomReservationsCntSqlRslt[0]['RESV_CNT']

        getRoomReservationsSql = f"SELECT * FROM {{EDW_DB_NAME}}.CIPINTEGRATIONODS.STG_CX_IB_WLV_HOTEL_RESV_LOAD"
        getRoomReservationsSqlRslt = session.sql(getRoomReservationsSql).collect()

        insertRoomReservationsVal = ''
        mongoDB_errors = []
        for resvRow in getRoomReservationsSqlRslt:
            if resvRow['CONFIRMATION_ID'] is not None:
                filterResvJson = {
                    "player_id":resvRow['PLAYER_ID'],
                    "confirmation_id":resvRow['CONFIRMATION_ID'],
                    }
                updateResvJson = {
                    "$set": {
                        "start_date":{ "$date": resvRow['START_DATE'] },
                        "end_date":{ "$date": resvRow['END_DATE'] },
                        "created_dtm":{ "$date": resvRow['CREATED_DTM'] },
                        "last_update_dtm":{ "$date": resvRow['LAST_UPDATE_DTM'] },
                        "site_id":{"$numberInt" : resvRow['SITE_ID']},
                        "status":resvRow['STATUS'],
                        "last_name":resvRow['LAST_NAME'],
                        "first_name":resvRow['FIRST_NAME'],
                        "email":resvRow['EMAIL'],
                        "phone_number":resvRow['PHONE_NUMBER'],
                        "room.reservation_id":resvRow['RESERVATION_ID'],
                        "room.source_system_code":resvRow['SOURCE_SYSTEM_CODE'],
                        "room.source_system_id":resvRow['SOURCE_SYSTEM_ID'],
                        "room.room_type":resvRow['ROOM_TYPE'],
                        "room.precheckin":resvRow['PRECHECKIN'],
                        "room.special_requests":resvRow['SPECIAL_REQUESTS'],
                        "room.occupants":{"$numberInt" : resvRow['OCCUPANTS']},
                        "room.adults":{"$numberInt" : resvRow['ADULTS']},
                        "room.children":{"$numberInt" : resvRow['CHILDREN']},
                        "room.market_code_first_day":resvRow['MARKET_CODE_FIRST_DAY'],
                        "room.rate_code_first_day":resvRow['RATE_CODE_FIRST_DAY'],
                        "room.block_code_first_day":resvRow['BLOCK_CODE_FIRST_DAY'],
                        "room.deposit_amount":{"$numberDouble" : resvRow['DEPOSIT_AMOUNT']},
                        "room.deposit_due_amount":{"$numberDouble" : resvRow['DEPOSIT_DUE_AMOUNT']},
                        "room.guarantee_due":resvRow['GUARANTEE_DUE'],
                        "room.company_booking":resvRow['COMPANY_BOOKING'],
                        "room.routing_room":resvRow['ROUTING_ROOM'],
                        "room.share_with_parent_id":resvRow['SHARE_WITH_PARENT_ID']
                    }
                }
            else:
                filterResvJson = {
                    "player_id":resvRow['PLAYER_ID'],
                    "room.reservation_id":resvRow['RESERVATION_ID']
                }
                updateResvJson = {
                    "$set": {
                        "start_date":{ "$date": resvRow['START_DATE'] },
                        "end_date":{ "$date": resvRow['END_DATE'] },
                        "created_dtm":{ "$date": resvRow['CREATED_DTM'] },
                        "last_update_dtm":{ "$date": resvRow['LAST_UPDATE_DTM'] },
                        "site_id":{"$numberInt" : resvRow['SITE_ID']},
                        "status":resvRow['STATUS'],
                        "last_name":resvRow['LAST_NAME'],
                        "first_name":resvRow['FIRST_NAME'],
                        "email":resvRow['EMAIL'],
                        "phone_number":resvRow['PHONE_NUMBER'],
                        "confirmation_id":resvRow['CONFIRMATION_ID'],
                        "room.source_system_code":resvRow['SOURCE_SYSTEM_CODE'],
                        "room.source_system_id":resvRow['SOURCE_SYSTEM_ID'],
                        "room.room_type":resvRow['ROOM_TYPE'],
                        "room.precheckin":resvRow['PRECHECKIN'],
                        "room.special_requests":resvRow['SPECIAL_REQUESTS'],                        
                        "room.occupants":{"$numberInt" : resvRow['OCCUPANTS']},
                        "room.adults":{"$numberInt" : resvRow['ADULTS']},
                        "room.children":{"$numberInt" : resvRow['CHILDREN']},
                        "room.market_code_first_day":resvRow['MARKET_CODE_FIRST_DAY'],
                        "room.rate_code_first_day":resvRow['RATE_CODE_FIRST_DAY'],
                        "room.block_code_first_day":resvRow['BLOCK_CODE_FIRST_DAY'],
                        "room.deposit_amount":{"$numberDouble" : resvRow['DEPOSIT_AMOUNT']},
                        "room.deposit_due_amount":{"$numberDouble" : resvRow['DEPOSIT_DUE_AMOUNT']},
                        "room.guarantee_due":resvRow['GUARANTEE_DUE'],
                        "room.company_booking":resvRow['COMPANY_BOOKING'],
                        "room.routing_room":resvRow['ROUTING_ROOM'],
                        "room.share_with_parent_id":resvRow['SHARE_WITH_PARENT_ID']
                    }
                }
            mongodb_payload = json.dumps({
                "dataSource": data_source,
                "database": database_name,
                "collection": collection_name,
                "filter": filterResvJson,
                "update": updateResvJson,
                "upsert": True
                }) 

            response = requests.post(mongodb_endpoint_url, data=mongodb_payload, headers=mongodb_endpoint_headers)

            # Something went wrong with Mongo DB post req
            if response.status_code not in (200,201):
                ERROR_PAYLOAD = {"RESERVATION_ID" : resvRow['RESERVATION_ID'], "ERROR_MSG" : response.text}
                mongoDB_errors.append(ERROR_PAYLOAD)

        log_msg('Mongo DB load completed', session)

        mongoDB_errors_list = json.dumps(mongoDB_errors).replace("'","''")

        updateBatchSql = f"""UPDATE {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_HOTEL_RESV_LOAD_STATUS
                              SET BATCH_STATUS='C',
                              EXTRACT_TO_TS = TO_TIMESTAMP('{maxUpdtTs}', 'YYYY-MM-DDTHH24:MI:SS.000Z'),
                              EXTRACT_COUNT = {roomReservationsCnt},
                              ERROR_LOG = '{mongoDB_errors_list}',
                              BATCH_END_TS = CURRENT_TIMESTAMP()
                              WHERE SITE_ID='WLV' and BATCH_STATUS = 'R'
                              """
        session.sql(updateBatchSql).collect()

        return f"Total reservations count: {roomReservationsCnt}"
    
    except Exception as e:
        errMsg = str(e)
        updateBatchSql = f"""UPDATE {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_HOTEL_RESV_LOAD_STATUS
                            SET BATCH_STATUS='F',
                            ERROR_LOG = '{errMsg}',
                            BATCH_END_TS = CURRENT_TIMESTAMP()
                            WHERE SITE_ID='WLV' and BATCH_STATUS = 'R'
                          """
        session.sql(updateBatchSql).collect()
        raise Exception(errMsg)

def log_msg(message, session):
    insert_log_query = f"""INSERT INTO  {{EDW_DB_NAME}}.CIPINTEGRATIONODS.AUDIT_CX_IB_LOG_TBL (SP_NAME, LOG_TS, LOG_MSG)
                          VALUES ('SP_CX_IB_WLV_HOTEL_RESV_LOAD', CURRENT_TIMESTAMP(), '{message}')"""
    session.sql(insert_log_query).collect()
