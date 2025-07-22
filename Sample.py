import requests

api_results = []  # To store results for writing back

venue_id = "ahhzfnNldmVucm9vbXMtc2VjdXJlLWRlbW9yHAsSD25pZ2h0bG9vcF9WZW51ZRiAgOixg9myCww"
url = f"https://demo.sevenrooms.com/api-ext/2_4/venues/{venue_id}/book"
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Authorization": "4942ada5c40cfc408ddb73a394af2cd8a351691f29ee4e9f27383bdc56d1051594fd77395415511a25f8fede5b451ea7d6d9a970e68ab04ccba8c41a77425cf5"
}

for row in rows:
    payload = {
        "date": str(row['DATE']),
        "time": row['TIME'],
        "party_size": str(row['PARTY_SIZE']),
        "first_name": row['FIRST_NAME'],
        "last_name": row['LAST_NAME'],
        "phone": row['PHONE'],
        "reservation_id": row['RESERVATION_ID'],
        "email": row['EMAIL'],
        "external_id": row['EXTERNAL_ID'],
        "external_user_id": row['EXTERNAL_USER_ID'],
        "send_client_email": "true",
        "send_client_sms": "false",
        "reservation_sms_opt_in": "true",
        "send_reminder_email": "true",
        "send_reminder_sms": "true",
        "venue_group_marketing_opt_in": "true",
        "venue_marketing_opt_in": "true",
        "is_walkin": "false",
        "duration": "60",
        "mf_ratio_male": "1",
        "mf_ratio_female": "1",
        "double_bookings_only_client_id_check": "true",
        "payment_by_paylink": "false",
        "paylink_cancel_time": "0"
    }

    try:
        response = requests.put(url, data=payload, headers=headers)
        response.raise_for_status()
        result = response.json()
        
        api_results.append({
            "reservation_id": row['RESERVATION_ID'],
            "confirmation_code": result.get("confirmation_code", ""),
            "status": result.get("status", ""),
            "table_id": result.get("table_id", ""),
            "reservation_url": result.get("reservation_url", "")
        })
        
    except Exception as e:
        print(f"❌ Error for reservation {row['RESERVATION_ID']}: {e}")








from snowflake.snowpark import Row

# Convert list of dicts to Snowpark DataFrame
response_rows = [Row(**r) for r in api_results]
response_df = session.create_dataframe(response_rows)






response_df.write.mode("overwrite").save_as_table("BOOKING_API_RESULTS")



session.sql("""
MERGE INTO BOOKINGS_TABLE tgt
USING BOOKING_API_RESULTS src
ON tgt.reservation_id = src.reservation_id
WHEN MATCHED THEN UPDATE SET
    tgt.confirmation_code = src.confirmation_code,
    tgt.status = src.status,
    tgt.table_id = src.table_id,
    tgt.reservation_url = src.reservation_url
""").collect()


{'code': 400, 'status': 400, 'msg': 'No shift found for date and time', 'request_id': '1753219088.688000ff1000ff00ff36906a1649a60001737e736576656e726f6f6d732d7365637572652d64656d6f00016170692d6578743a636972636c6563692d3063386133346265313900010156'}
