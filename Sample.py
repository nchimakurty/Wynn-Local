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
        "notes": row['BKFNOTES']
    }

    try:
        response = requests.put(url, data=payload, headers=headers)
        result = response.json()

        # Prepare uniform result structure
        record = {
            "GRIDNUM": row['GRIDNUM'],
            "PLAYERID": row['PLAYERID'],
            "reservation_id": result.get("reservation_id", ""),
            "reservation_reference_code": result.get("reservation_reference_code", ""),
            "client_id": result.get("client_id", ""),
            "client_reference_code": result.get("client_reference_code", ""),
            "msg": result.get("msg", ""),
            "request_id": result.get("request_id", ""),
            "status_code": response.status_code
        }

        api_results.append(record)

    except requests.exceptions.RequestException as e:
        # Handle hard errors like timeouts
        api_results.append({
            "GRIDNUM": row['GRIDNUM'],
            "PLAYERID": row['PLAYERID'],
            "reservation_id": "",
            "reservation_reference_code": "",
            "client_id": "",
            "client_reference_code": "",
            "msg": str(e),
            "request_id": "",
            "status_code": "RequestException"
        })

print(api_results)
