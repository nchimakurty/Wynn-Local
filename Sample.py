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
        # response.raise_for_status()
        result = response.json()

        if response.status_code == 200:
          # Handle successful response
          api_results.append({
              "reservation_id": result.get("reservation_id", ""),
              "reservation_reference_code": result.get("reservation_reference_code", ""),
              "client_id": result.get("client_id", ""),
              "client_reference_code": result.get("client_reference_code", "")
          })      
        else:
          # Error handling
          api_results.append({
              "msg": result.get("msg", ""),
              "request_id": result.get("request_id", "")
          })            
          
    except requests.exceptions.RequestException as e:
        # Catch hard failures only (e.g., timeout, DNS failure)
        print(str(e))        
        
print(api_results)
