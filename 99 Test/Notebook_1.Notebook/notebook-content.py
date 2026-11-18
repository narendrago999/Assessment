# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests

base_url = "https://hapi.fhir.org/baseR4"

patient_ids = [
    "99537",
    "358260",
    "733019",
    "c19264dc-4d8e-488f-b6df-31a896089080",
    "example"
]

for patient_id in patient_ids:
    url = f"{base_url}/Patient/{patient_id}"

    try:
        response = requests.get(
            url,
            headers={"Accept": "application/fhir+json"},
            timeout=30
        )

        print(f"\nPatient ID: {patient_id}")
        print(f"HTTP Status: {response.status_code}")

        if response.status_code == 200:
            resource = response.json()
            print(f"Resource Type: {resource.get('resourceType')}")
            print(f"Returned ID: {resource.get('id')}")
        else:
            print(response.text[:500])

    except Exception as exc:
        print(f"Request error: {exc}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
