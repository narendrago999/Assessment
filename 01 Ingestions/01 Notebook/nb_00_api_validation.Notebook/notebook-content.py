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
from datetime import datetime, timezone

BASE_URL = "https://hapi.fhir.org/baseR4"

RESOURCES = [
    "Patient",
    "Encounter",
    "Observation",
    "Condition"
]

session = requests.Session()
session.headers.update({
    "Accept": "application/fhir+json",
    "User-Agent": "Synapx-FHIR-Engineering-Assessment/1.0"
})

print("FHIR API connectivity validation")
print("Execution time:", datetime.now(timezone.utc).isoformat())

# Validate FHIR server metadata
metadata_url = f"{BASE_URL}/metadata"

response = session.get(
    metadata_url,
    timeout=60
)

response.raise_for_status()

metadata = response.json()

assert metadata.get("resourceType") == "CapabilityStatement", (
    "Unexpected FHIR metadata response"
)

print("FHIR server connection successful")

# Validate each resource endpoint
validation_results = []

for resource in RESOURCES:

    url = f"{BASE_URL}/{resource}"

    try:
        response = session.get(
            url,
            params={"_count": 1},
            timeout=60
        )

        response.raise_for_status()

        bundle = response.json()

        result = {
            "resource": resource,
            "status": response.status_code,
            "resource_type": bundle.get("resourceType"),
            "total": bundle.get("total"),
            "entries_returned": len(bundle.get("entry", [])),
            "success": True
        }

        validation_results.append(result)

        print(f"SUCCESS: {resource}", result)

    except Exception as error:

        validation_results.append({
            "resource": resource,
            "success": False,
            "error": str(error)
        })

        print(f"FAILED: {resource}", str(error))

failed = [
    item for item in validation_results
    if not item["success"]
]

if failed:
    raise Exception(
        f"FHIR endpoint validation failed: {failed}"
    )

print("\nAll four FHIR resource endpoints validated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
