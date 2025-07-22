import requests

venue_id = "ahhzfnNldmVucm9vbXMtc2VjdXJlLWRlbW9yHAsSD25pZ2h0bG9vcF9WZW51ZRiAgOixg9myCww"
url = "https://demo.sevenrooms.com/api-ext/2_4/venues/" + venue_id + "/book"

payload = {
  "date": "2019-08-24",
  "time": "string",
  "party_size": "1",
  "first_name": "John",
  "last_name": "Last name of the Client booking the Reservation.",
  "phone": "string",
  "reservation_id": "string",
  "reservation_hold_id": "string",
  "email": "support@sevenrooms.com",
  "notes": "string",
  "internal_notes": "string",
  "duration": "60",
  "table": "string",
  "tags": "string",
  "salutation": "string",
  "birthday": "string",
  "anniversary": "string",
  "address": "string",
  "address2": "string",
  "city": "string",
  "state": "string",
  "country": "string",
  "postal_code": "string",
  "external_id": "55602",
  "external_user_id": "55602",
  "send_client_email": "true",
  "send_client_sms": "false",
  "loyalty_id": "`loyalty_id` value from the Client entity associated with the Reservation.",
  "loyalty_tier": "string",
  "loyalty_rank": "`loyalty_rank` value from the Client entity.",
  "is_walkin": "true",
  "client_id": "string",
  "access_persistent_id": "string",
  "reservation_sms_opt_in": "true",
  "send_reminder_email": "true",
  "send_reminder_sms": "true",
  "venue_group_marketing_opt_in": "true",
  "venue_marketing_opt_in": "true",
  "(Legacy) upgrade_categories": "string",
  "upgrade_inventories": "string",
  "bypass_availability": "false",
  "bypass_required_contact_fields": "false",
  "bypass_duplicate_reservation_check": "false",
  "bypass_editable_cutoff": "false",
  "mf_ratio_male": "2",
  "mf_ratio_female": "2",
  "add_client_tags": "string",
  "remove_client_tags": "string",
  "double_bookings_only_client_id_check": "true",
  "payment_by_paylink": "true",
  "paylink_cancel_time": "0",
  "paylink_note": "string",
  "paylink_email": "string"
}

headers = {
  "Content-Type": "application/x-www-form-urlencoded",
  "Authorization": "4942ada5c40cfc408ddb73a394af2cd8a351691f29ee4e9f27383bdc56d1051594fd77395415511a25f8fede5b451ea7d6d9a970e68ab04ccba8c41a77425cf5"
}

response = requests.put(url, data=payload, headers=headers)

data = response.json()
print(data)
