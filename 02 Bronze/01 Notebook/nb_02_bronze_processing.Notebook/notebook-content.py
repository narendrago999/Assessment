# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "34da9e7d-0b43-4a6f-a572-40b1fbb7963a",
# META       "default_lakehouse_name": "lh_fhir_bronze",
# META       "default_lakehouse_workspace_id": "8edaab9a-2151-418e-9fef-63e0921486fc",
# META       "known_lakehouses": [
# META         {
# META           "id": "34da9e7d-0b43-4a6f-a572-40b1fbb7963a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# ================================================================
# SYNAPX FHIR DATA ENGINEERING
# NOTEBOOK: nb_02_bronze_processing
# PLATFORM: Microsoft Fabric - PySpark
# LAYER: BRONZE
#
# INPUT:
#   Files/raw_fhir/ (OneLake shortcut to Raw Lakehouse)
#
# OUTPUT:
#   bronze_patient
#   bronze_encounter
#   bronze_observation
#   bronze_condition
#   bronze_ingestion_audit
#
# DESIGN:
#   - Preserve original Raw files
#   - Extract individual FHIR resources
#   - Preserve source and ingestion lineage
#   - Retain historical ingestion occurrences
#   - Use idempotent Delta MERGE writes
# ================================================================


# ================================================================
# 1. IMPORTS
# ================================================================

import json
import logging

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable


# ================================================================
# 2. CONFIGURATION
# ================================================================

RAW_ROOT = "Files/raw_fhir"

RESOURCES = [
    "Patient",
    "Encounter",
    "Observation",
    "Condition"
]

BRONZE_TABLES = {
    "Patient": "bronze_patient",
    "Encounter": "bronze_encounter",
    "Observation": "bronze_observation",
    "Condition": "bronze_condition"
}

BRONZE_LOADED_AT = datetime.now(timezone.utc).isoformat()

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("FHIR_BRONZE")

print("FHIR Bronze processing started")
print("Raw shortcut:", RAW_ROOT)
print("Execution time:", BRONZE_LOADED_AT)


# ================================================================
# 3. VERIFY LAKEHOUSE SHORTCUT
# ==============================================================

if not notebookutils.fs.exists(RAW_ROOT):
    raise RuntimeError(
        f"Raw shortcut not found: {RAW_ROOT}. "
        "Verify that raw_fhir is attached to the Bronze Lakehouse."
    )

print("Raw shortcut verified")


# ================================================================
# 4. LOAD RAW FHIR BUNDLE FILES
# ================================================================

# The Raw ingestion notebook creates:
# page_00001.json
# page_00001.metadata.json
#
# Five question marks match exactly five digits.
# This prevents metadata files from being read as Bundle files.

raw_df = (
    spark.read
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "page_?????.json")
    .json(RAW_ROOT)
    .withColumn("_raw_file_path", F.input_file_name())
)

raw_df = raw_df.filter(
    F.col("resourceType") == "Bundle"
)

if raw_df.limit(1).count() == 0:
    raise RuntimeError(
        "No FHIR Bundle files found in the Raw shortcut."
    )

print("Raw Bundle files loaded")

raw_file_count = raw_df.select(
    "_raw_file_path"
).distinct().count()

print("Raw Bundle file count:", raw_file_count)


# ================================================================
# 5. LOAD PAGE METADATA SIDECARS
# ================================================================

metadata_df = (
    spark.read
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "page_?????.metadata.json")
    .json(RAW_ROOT)
    .withColumn(
        "_metadata_file_path",
        F.input_file_name()
    )
)

# Convert:
# page_00001.metadata.json
# to:
# page_00001.json

metadata_df = metadata_df.withColumn(
    "_matching_raw_path",
    F.regexp_replace(
        F.col("_metadata_file_path"),
        r"\.metadata\.json$",
        ".json"
    )
)

metadata_df = metadata_df.select(
    F.col("_matching_raw_path"),
    F.col("run_id"),
    F.col("resource"),
    F.col("page_number"),
    F.col("extraction_timestamp"),
    F.col("api_call_timestamp"),
    F.col("data_saved_timestamp"),
    F.col("api_url_or_params"),
    F.col("response_status"),
    F.col("entries_returned"),
    F.col("fhir_bundle_total"),
    F.col("request_params"),
    F.col("raw_file_path").alias(
        "original_raw_file_path"
    )
)

metadata_count = metadata_df.count()

print("Metadata sidecar count:", metadata_count)


# ================================================================
# 6. JOIN RAW BUNDLES WITH METADATA
# ================================================================

joined_df = (
    raw_df.alias("raw")
    .join(
        metadata_df.alias("meta"),
        F.col("raw._raw_file_path")
        == F.col("meta._matching_raw_path"),
        "left"
    )
)

# Metadata is required for lineage and auditing.
missing_metadata = joined_df.filter(
    F.col("meta.run_id").isNull()
).count()

if missing_metadata > 0:
    raise RuntimeError(
        f"{missing_metadata} raw Bundle files "
        "do not have matching metadata sidecars."
    )

print("Raw Bundle and metadata join successful")


# ================================================================
# 7. EXPLODE BUNDLE ENTRIES
# ================================================================

# Each Bundle may contain multiple resources.
# posexplode retains the original position of each entry.

exploded_df = joined_df.select(
    F.col("raw._raw_file_path").alias("raw_file_path"),

    F.col("meta.run_id").alias("run_id"),

    F.col("meta.resource").alias("requested_resource"),

    F.col("meta.page_number").alias("page_number"),

    F.col("meta.extraction_timestamp").alias(
        "extraction_timestamp"
    ),

    F.col("meta.api_call_timestamp").alias(
        "api_call_timestamp"
    ),

    F.col("meta.data_saved_timestamp").alias(
        "data_saved_timestamp"
    ),

    F.col("meta.api_url_or_params").alias(
        "api_url_or_params"
    ),

    F.posexplode(F.col("raw.entry")).alias(
        "entry_index",
        "entry"
    )
)

resource_df = exploded_df.select(
    "raw_file_path",
    "run_id",
    "requested_resource",
    "page_number",
    "entry_index",
    "extraction_timestamp",
    "api_call_timestamp",
    "data_saved_timestamp",
    "api_url_or_params",

    F.col("entry.resource").alias("resource")
).filter(
    F.col("resource").isNotNull()
)


# ================================================================
# 8. EXTRACT RESOURCE FIELDS
# ================================================================

bronze_df = resource_df.select(

    F.col("resource.id").cast("string").alias(
        "resource_id"
    ),

    F.col("resource.resourceType").cast("string").alias(
        "resource_type"
    ),

    F.col("resource.meta.versionId").cast("string").alias(
        "source_version_id"
    ),

    F.col("resource.meta.lastUpdated").cast("string").alias(
        "source_last_updated"
    ),

    F.col("extraction_timestamp").cast("string").alias(
        "extraction_timestamp"
    ),

    F.col("api_url_or_params").cast("string").alias(
        "api_url_or_params"
    ),

    F.col("api_call_timestamp").cast("string").alias(
        "api_call_timestamp"
    ),

    F.col("data_saved_timestamp").cast("string").alias(
        "data_saved_timestamp"
    ),

    F.col("run_id").cast("string").alias(
        "run_id"
    ),

    F.to_json(
        F.col("resource")
    ).alias("raw_resource_json"),

    F.col("raw_file_path").cast("string").alias(
        "raw_file_path"
    ),

    F.col("page_number").cast("integer").alias(
        "page_number"
    ),

    F.col("entry_index").cast("integer").alias(
        "entry_index"
    ),

    F.current_timestamp().alias(
        "bronze_loaded_at"
    )
)


# ================================================================
# 9. BASIC DATA VALIDATION
# ================================================================

# Reject records without a resource ID or resource type.

invalid_records = bronze_df.filter(
    F.col("resource_id").isNull()
    | F.col("resource_type").isNull()
    | F.col("raw_resource_json").isNull()
)

invalid_count = invalid_records.count()

if invalid_count > 0:
    raise RuntimeError(
        f"Found {invalid_count} invalid FHIR resource records."
    )

# Verify the resource type against the requested endpoint.

wrong_type_records = bronze_df.filter(
    F.col("resource_type") != F.col("requested_resource")
).count() if "requested_resource" in bronze_df.columns else 0

# requested_resource was intentionally not included in the
# projected bronze_df above. Validate the supported resource set.

unexpected_types = bronze_df.filter(
    ~F.col("resource_type").isin(RESOURCES)
).count()

if unexpected_types > 0:
    raise RuntimeError(
        f"Found {unexpected_types} unsupported FHIR resource types."
    )

print("Bronze record validation successful")


# ================================================================
# 10. CREATE DETERMINISTIC RECORD KEY
# ================================================================

# Keep separate ingestion occurrences and source versions.
# This permits historical source versions to remain traceable.

bronze_df = bronze_df.withColumn(
    "record_key",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("run_id"), F.lit("")),
            F.coalesce(F.col("resource_type"), F.lit("")),
            F.coalesce(F.col("resource_id"), F.lit("")),
            F.coalesce(F.col("source_version_id"), F.lit("")),
            F.coalesce(F.col("raw_file_path"), F.lit("")),
            F.coalesce(
                F.col("page_number").cast("string"),
                F.lit("")
            ),
            F.coalesce(
                F.col("entry_index").cast("string"),
                F.lit("")
            )
        ),
        256
    )
)


# ================================================================
# 11. DELTA TABLE UPSERT FUNCTION
# ==============================================================

def write_delta_idempotent(df, table_name):

    if not spark.catalog.tableExists(table_name):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

        print(
            f"Created Delta table: {table_name}"
        )

    else:

        delta_table = DeltaTable.forName(
            spark,
            table_name
        )

        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.record_key = source.record_key"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(
            f"Idempotent Delta merge completed: {table_name}"
        )


# ================================================================
# 12. WRITE RESOURCE-SPECIFIC BRONZE TABLES
# ==============================================================

resource_counts = []

for resource in RESOURCES:

    table_name = BRONZE_TABLES[resource]

    resource_data = bronze_df.filter(
        F.col("resource_type") == resource
    )

    count = resource_data.count()

    print(
        f"{resource}: {count} records"
    )

    if count == 0:
        print(
            f"WARNING: No records received for {resource}. "
            "Creating an empty Delta table if it does not exist."
        )

    if not spark.catalog.tableExists(table_name):

        (
            resource_data.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

    elif count > 0:

        write_delta_idempotent(
            resource_data,
            table_name
        )

    resource_counts.append({
        "resource": resource,
        "table": table_name,
        "records_processed": count
    })


# ================================================================
# 13. CREATE BRONZE INGESTION AUDIT TABLE
# ==============================================================

audit_df = metadata_df.select(

    F.col("run_id"),

    F.col("resource"),

    F.col("page_number"),

    F.col("extraction_timestamp"),

    F.col("api_call_timestamp"),

    F.col("data_saved_timestamp"),

    F.col("api_url_or_params"),

    F.col("response_status"),

    F.col("entries_returned"),

    F.col("fhir_bundle_total"),

    F.col("original_raw_file_path").alias(
        "raw_file_path"
    ),

    F.current_timestamp().alias(
        "bronze_audit_loaded_at"
    )
)

audit_df = audit_df.withColumn(
    "record_key",
    F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("run_id"), F.lit("")),
            F.coalesce(F.col("resource"), F.lit("")),
            F.coalesce(
                F.col("page_number").cast("string"),
                F.lit("")
            ),
            F.coalesce(F.col("raw_file_path"), F.lit(""))
        ),
        256
    )
)

write_delta_idempotent(
    audit_df,
    "bronze_ingestion_audit"
)


# ================================================================
# 14. FINAL BRONZE EXECUTION SUMMARY
# ==============================================================

print("\n" + "=" * 70)
print("FHIR BRONZE PROCESSING SUMMARY")
print("=" * 70)

for item in resource_counts:

    print(
        f"{item['resource']} | "
        f"{item['table']} | "
        f"{item['records_processed']} records"
    )

total_records = bronze_df.count()

print("\nTotal Bronze resource records:", total_records)

print(
    "Audit records:",
    audit_df.count()
)

print(
    "Bronze processing timestamp:",
    BRONZE_LOADED_AT
)

print("\nBronze processing completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
