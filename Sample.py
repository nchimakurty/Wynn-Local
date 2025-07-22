# Collect Snowpark rows to local memory
rows = df.collect()

# Loop through each row and send as API request
for idx, row in enumerate(rows):
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
        print(f"✅ Success [{idx}]:", response.json())
    except requests.exceptions.RequestException as e:
        print(f"❌ Error [{idx}]: {e}")
        print("Response:", response.text)
