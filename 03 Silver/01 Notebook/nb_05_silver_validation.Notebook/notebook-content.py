# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d6aa32fb-d2a9-4b2c-8d91-dff03858f295",
# META       "default_lakehouse_name": "lh_fhir_silver",
# META       "default_lakehouse_workspace_id": "8edaab9a-2151-418e-9fef-63e0921486fc",
# META       "known_lakehouses": [
# META         {
# META           "id": "d6aa32fb-d2a9-4b2c-8d91-dff03858f295"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# ================================================================
# SYNAPX FHIR DATA ENGINEERING
# NOTEBOOK: nb_05_silver_validation
# LAYER: SILVER DATA QUALITY AND SCD2 VALIDATION
# ================================================================

from pyspark.sql import functions as F

TABLES = {
    "Patient": "dbo.silver_patient",
    "Encounter": "dbo.silver_encounter",
    "Observation": "dbo.silver_observation",
    "Condition": "dbo.silver_condition"
}

results = []

print("Starting Silver validation")


# ------------------------------------------------
# 1. TABLE EXISTENCE
# ------------------------------------------------

for resource, table in TABLES.items():

    if not spark.catalog.tableExists(table):
        raise RuntimeError(
            f"Silver table missing: {table}"
        )

    print("PASS: Table exists:", table)


# ------------------------------------------------
# 2. RECORD COUNTS
# ------------------------------------------------

for resource, table in TABLES.items():

    df = spark.table(table)

    total = df.count()

    current = df.filter(
        F.col("is_current") == True
    ).count()

    historical = df.filter(
        F.col("is_current") == False
    ).count()

    results.append({
        "resource": resource,
        "table": table,
        "total_versions": total,
        "current_records": current,
        "historical_records": historical
    })

    print(
        f"{resource}: Total={total}, "
        f"Current={current}, "
        f"Historical={historical}"
    )


# ------------------------------------------------
# 3. RESOURCE KEY VALIDATION
# ------------------------------------------------

for resource, table in TABLES.items():

    df = spark.table(table)

    invalid = df.filter(
        F.col("resource_id").isNull()
        | (F.length(F.trim(F.col("resource_id"))) == 0)
    ).count()

    if invalid > 0:
        raise RuntimeError(
            f"{table}: Invalid resource IDs: {invalid}"
        )

    print("PASS: Resource IDs:", table)


# ------------------------------------------------
# 4. CURRENT VERSION UNIQUENESS
# ------------------------------------------------

for resource, table in TABLES.items():

    current_df = (
        spark.table(table)
        .filter(F.col("is_current") == True)
    )

    duplicates = (
        current_df.groupBy("resource_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicates > 0:
        raise RuntimeError(
            f"{table}: Multiple current versions "
            f"found for {duplicates} resource IDs."
        )

    print("PASS: One current version:", table)


# ------------------------------------------------
# 5. CURRENT VALIDITY CHECK
# ------------------------------------------------

for resource, table in TABLES.items():

    invalid = spark.table(table).filter(
        (F.col("is_current") == True)
        & F.col("valid_to").isNotNull()
    ).count()

    if invalid > 0:
        raise RuntimeError(
            f"{table}: Current records have valid_to values."
        )

    print("PASS: Current validity:", table)


# ------------------------------------------------
# 6. HISTORICAL VALIDITY CHECK
# ------------------------------------------------

for resource, table in TABLES.items():

    invalid = spark.table(table).filter(
        (F.col("is_current") == False)
        & (
            F.col("valid_to").isNull()
            | (F.col("valid_to") <= F.col("valid_from"))
        )
    ).count()

    if invalid > 0:
        raise RuntimeError(
            f"{table}: Invalid historical validity periods: "
            f"{invalid}"
        )

    print("PASS: Historical validity:", table)


# ------------------------------------------------
# 7. HASH VALIDATION
# ------------------------------------------------

for resource, table in TABLES.items():

    invalid = spark.table(table).filter(
        F.col("record_hash").isNull()
        | F.col("valid_from").isNull()
    ).count()

    if invalid > 0:
        raise RuntimeError(
            f"{table}: Missing record hash or valid_from."
        )

    print("PASS: Hash and validity fields:", table)


# ------------------------------------------------
# 8. SOURCE LINEAGE VALIDATION
# ------------------------------------------------

for resource, table in TABLES.items():

    invalid = spark.table(table).filter(
        F.col("run_id").isNull()
        | F.col("raw_file_path").isNull()
        | F.col("extraction_timestamp").isNull()
    ).count()

    if invalid > 0:
        raise RuntimeError(
            f"{table}: Missing source lineage: {invalid}"
        )

    print("PASS: Source lineage:", table)


# ------------------------------------------------
# 9. CURRENT VERSION COUNT VALIDATION
# ------------------------------------------------

for resource, table in TABLES.items():

    df = spark.table(table)

    no_current = (
        df.select("resource_id").distinct()
        .join(
            df.filter(
                F.col("is_current") == True
            ).select("resource_id").distinct(),
            "resource_id",
            "left_anti"
        )
        .count()
    )

    if no_current > 0:
        raise RuntimeError(
            f"{table}: {no_current} resource IDs "
            "have no current version."
        )

    print("PASS: Current version availability:", table)


# ------------------------------------------------
# 10. DISPLAY SUMMARY
# ------------------------------------------------

summary_df = spark.createDataFrame(results)

display(summary_df)

print("\n======================================")
print("SILVER VALIDATION COMPLETED SUCCESSFULLY")
print("======================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
