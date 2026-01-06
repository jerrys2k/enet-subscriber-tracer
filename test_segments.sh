#!/bin/bash

# === CONFIG ===
BASE_URL="https://prd-enet-core-prc-api-h8xpg6.a9tihy.usa-e2.cloudhub.io/api/v1/sf/accounts"
SUBSCRIBER_ID="6fe58d55-a8f1-477e-a742-10e18b457842"
CLIENT_ID="7694144c173b4e33b9ef07b704fec733"
CLIENT_SECRET="E99C70b938Fa4d64998464D3Ff02FdC3"

# === SEGMENTS TO TRY ===
segments=("PROFILE" "SUBSCRIPTION" "DEVICE" "NETWORK" "ALL" "IMSI" "SIM")

echo "🔁 Testing segments for subscriber ID: $SUBSCRIBER_ID"
echo

for segment in "${segments[@]}"; do
  echo "🔍 Trying segment: $segment"
  response=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X GET "$BASE_URL/$SUBSCRIBER_ID?segment=$segment" \
    -H "client_id: $CLIENT_ID" \
    -H "client_secret: $CLIENT_SECRET")

  # Extract HTTP status
  http_status=$(echo "$response" | sed -n 's/.*HTTP_STATUS://p')
  body=$(echo "$response" | sed '/HTTP_STATUS:/d')

  if [[ "$http_status" == "200" ]]; then
    echo "✅ Success (HTTP 200):"
    echo "$body" | jq
  else
    echo "❌ Failed (HTTP $http_status)"
    echo "Reason: $(echo "$body" | jq -r '.error.message // .error.description // "Unknown error"')"
  fi

  echo "------------------------------------------------------"
done
