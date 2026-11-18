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
# SYNAPX FHIR BRONZE DATA QUALITY VALIDATION
# NOTEBOOK: nb_03_bronze_validation
# ================================================================

from pyspark.sql import functions as F

RESOURCES = [
    "Patient",
    "Encounter",
    "Observation",
    "Condition"
]

TABLES = {
    "Patient": "bronze_patient",
    "Encounter": "bronze_encounter",
    "Observation": "bronze_observation",
    "Condition": "bronze_condition"
}

results = []

print("Starting Bronze validation")


# ------------------------------------------------
# 1. TABLE EXISTENCE
# ------------------------------------------------

for resource, table in TABLES.items():

    if not spark.catalog.tableExists(table):
        raise RuntimeError(
            f"Missing Bronze table: {table}"
        )

    print("PASS: Table exists:", table)


# ------------------------------------------------
# 2. RECORD COUNTS
# ------------------------------------------------

for resource, table in TABLES.items():

    df = spark.table(table)

    count = df.count()

    results.append({
        "resource": resource,
        "table": table,
        "record_count": count
    })

    print(
        f"{resource}: {count} records"
    )


# ------------------------------------------------
# 3. NULL RESOURCE ID CHECK
# ------------------------------------------------

for resource, table in TABLES.items():

    df = spark.table(table)

    null_count = df.filter(
        F.col("resource_id").isNull()
    ).count()

    if null_count > 0:
        raise RuntimeError(
            f"{table}: {null_count} null resource IDs"
        )

    print("PASS: Resource IDs:", table)


# ------------------------------------------------
# 4. NULL RAW JSON CHECK
# ------------------------------------------------

for resource, table in TABLES.items():

    df = spark.table(table)

    invalid_count = df.filter(
        F.col("raw_resource_json").isNull()
    ).count()

    if invalid_count > 0:
        raise RuntimeError(
            f"{table}: missing raw resource JSON"
        )

    print("PASS: Raw JSON:", table)


# ------------------------------------------------
# 5. RECORD KEY DUPLICATION CHECK
# ------------------------------------------------

for resource, table in TABLES.items():

    df = spark.table(table)

    duplicate_count = (
        df.groupBy("record_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            f"{table}: duplicate record keys found"
        )

    print("PASS: Record keys:", table)


# ------------------------------------------------
# 6. SOURCE LINEAGE CHECK
# ------------------------------------------------

for resource, table in TABLES.items():

    df = spark.table(table)

    missing_lineage = df.filter(
        F.col("run_id").isNull()
        | F.col("raw_file_path").isNull()
        | F.col("extraction_timestamp").isNull()
        | F.col("data_saved_timestamp").isNull()
    ).count()

    if missing_lineage > 0:
        raise RuntimeError(
            f"{table}: {missing_lineage} records "
            "are missing lineage metadata"
        )

    print("PASS: Source lineage:", table)


# ------------------------------------------------
# 7. AUDIT TABLE VALIDATION
# ------------------------------------------------

audit_table = "bronze_ingestion_audit"

if not spark.catalog.tableExists(audit_table):
    raise RuntimeError(
        "Bronze ingestion audit table is missing"
    )

audit_df = spark.table(audit_table)

audit_count = audit_df.count()

if audit_count == 0:
    raise RuntimeError(
        "Bronze ingestion audit table is empty"
    )

print("PASS: Audit table")
print("Audit records:", audit_count)


# ------------------------------------------------
# 8. RESOURCE TYPE VALIDATION
# ------------------------------------------------

for resource, table in TABLES.items():

    df = spark.table(table)

    invalid_type_count = df.filter(
        F.col("resource_type") != resource
    ).count()

    if invalid_type_count > 0:
        raise RuntimeError(
            f"{table}: unexpected resource types found"
        )

    print("PASS: Resource type:", resource)


# ------------------------------------------------
# 9. DISPLAY VALIDATION RESULTS
# ------------------------------------------------

result_df = spark.createDataFrame(results)

display(result_df)

print("======================================")
print("BRONZE DATA QUALITY VALIDATION PASSED")
print("======================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
