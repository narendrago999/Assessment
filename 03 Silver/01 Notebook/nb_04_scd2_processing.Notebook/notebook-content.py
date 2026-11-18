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
# NOTEBOOK: nb_04_scd2_processing
# LAYER: SILVER - SCD TYPE 2
# PLATFORM: MICROSOFT FABRIC / PYSPARK / DELTA LAKE
# ================================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timezone

# ------------------------------------------------
# 1. CONFIGURATION
# ------------------------------------------------

RESOURCES = {
    "Patient": {
        "stage": "dbo.silver_stage_patient",
        "target": "dbo.silver_patient"
    },
    "Encounter": {
        "stage": "dbo.silver_stage_encounter",
        "target": "dbo.silver_encounter"
    },
    "Observation": {
        "stage": "dbo.silver_stage_observation",
        "target": "dbo.silver_observation"
    },
    "Condition": {
        "stage": "dbo.silver_stage_condition",
        "target": "dbo.silver_condition"
    }
}

PROCESSING_TIMESTAMP = datetime.now(
    timezone.utc
).isoformat()

print("SCD Type 2 processing started")
print("Processing timestamp:", PROCESSING_TIMESTAMP)


# ------------------------------------------------
# 2. HELPER: PREPARE SOURCE DATA
# ------------------------------------------------

def prepare_source(stage_table):

    if not spark.catalog.tableExists(stage_table):
        raise RuntimeError(
            f"Stage table does not exist: {stage_table}. "
            "Run nb_03_silver_transformation first."
        )

    source = spark.table(stage_table)

    required = [
        "resource_id",
        "record_hash",
        "extraction_timestamp"
    ]

    missing = [
        column for column in required
        if column not in source.columns
    ]

    if missing:
        raise RuntimeError(
            f"{stage_table} missing columns: {missing}"
        )

    # Prefer source update timestamp.
    # If absent, use extraction timestamp.

    source = source.withColumn(
        "valid_from",
        F.coalesce(
            F.col("source_last_updated"),
            F.col("extraction_timestamp")
        ).cast("timestamp")
    )

    source = source.filter(
        F.col("resource_id").isNotNull()
        & F.col("record_hash").isNotNull()
        & F.col("valid_from").isNotNull()
    )

    # Ensure only one incoming record per business key.

    duplicate_keys = (
        source.groupBy("resource_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_keys > 0:
        raise RuntimeError(
            f"{stage_table}: duplicate resource IDs "
            f"found: {duplicate_keys}"
        )

    return source


# ------------------------------------------------
# 3. HELPER: INITIALIZE SCD2 TABLE
# ------------------------------------------------

def initialize_scd2_table(source, target_table):

    initial = (
        source
        .withColumn(
            "valid_to",
            F.lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            F.lit(True)
        )
        .withColumn(
            "created_at",
            F.current_timestamp()
        )
        .withColumn(
            "updated_at",
            F.current_timestamp()
        )
    )

    (
        initial.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table)
    )

    print(
        f"Initialized SCD2 table: {target_table}"
    )


# ------------------------------------------------
# 4. HELPER: APPLY SCD TYPE 2
# ------------------------------------------------

def apply_scd2(stage_table, target_table):

    print("\nProcessing:", target_table)

    source = prepare_source(stage_table)

    source_count = source.count()

    print("Incoming source records:", source_count)

    if source_count == 0:
        print(
            f"No records to process for {target_table}"
        )
        return {
            "table": target_table,
            "incoming": 0,
            "expired": 0,
            "inserted": 0
        }

    # --------------------------------------------
    # INITIAL LOAD
    # --------------------------------------------

    if not spark.catalog.tableExists(target_table):

        initialize_scd2_table(
            source,
            target_table
        )

        return {
            "table": target_table,
            "incoming": source_count,
            "expired": 0,
            "inserted": source_count
        }

    # --------------------------------------------
    # READ CURRENT TARGET VERSIONS
    # --------------------------------------------

    target_delta = DeltaTable.forName(
        spark,
        target_table
    )

    target_df = spark.table(target_table)

    current_df = target_df.filter(
        F.col("is_current") == True
    )

    # --------------------------------------------
    # EXPIRE CHANGED CURRENT RECORDS
    # --------------------------------------------

    # Only expire when the incoming version is newer
    # and its business content hash has changed.
    #
    # Older late-arriving versions must not replace
    # a newer current record.

    (
        target_delta.alias("target")
        .merge(
            source.alias("source"),
            """
            target.resource_id = source.resource_id
            AND target.is_current = true
            """
        )
        .whenMatchedUpdate(
            condition="""
                source.record_hash <> target.record_hash
                AND source.valid_from > target.valid_from
            """,
            set={
                "valid_to": "source.valid_from",
                "is_current": "false",
                "updated_at": "current_timestamp()"
            }
        )
        .execute()
    )

    print("Expired previous changed versions where applicable")

    # --------------------------------------------
    # IDENTIFY NEW OR CHANGED RECORDS
    # --------------------------------------------

    # Read the active versions after the expiration step.

    current_after_expiry = (
        spark.table(target_table)
        .filter(F.col("is_current") == True)
        .select(
            "resource_id",
            "record_hash",
            "valid_from"
        )
    )

    comparison = source.alias("source").join(
        current_after_expiry.alias("target"),
        F.col("source.resource_id")
        == F.col("target.resource_id"),
        "left"
    )

    insert_candidates = comparison.filter(
        F.col("target.resource_id").isNull()
        |
        (
            (F.col("source.record_hash")
             != F.col("target.record_hash"))
            &
            (F.col("source.valid_from")
             > F.col("target.valid_from"))
        )
    )

    # Select all source fields without target comparison fields.

    source_columns = source.columns

    insert_candidates = insert_candidates.select(
        *[
            F.col(f"source.{column}").alias(column)
            for column in source_columns
        ]
    )

    inserted_count = insert_candidates.count()

    # --------------------------------------------
    # INSERT NEW CURRENT VERSIONS
    # --------------------------------------------

    if inserted_count > 0:

        new_versions = (
            insert_candidates
            .withColumn(
                "valid_to",
                F.lit(None).cast("timestamp")
            )
            .withColumn(
                "is_current",
                F.lit(True)
            )
            .withColumn(
                "created_at",
                F.current_timestamp()
            )
            .withColumn(
                "updated_at",
                F.current_timestamp()
            )
        )

        (
            new_versions.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )

    print("New versions inserted:", inserted_count)

    # --------------------------------------------
    # FINAL COUNTS
    # --------------------------------------------

    final_df = spark.table(target_table)

    current_count = final_df.filter(
        F.col("is_current") == True
    ).count()

    historical_count = final_df.filter(
        F.col("is_current") == False
    ).count()

    print("Current records:", current_count)
    print("Historical records:", historical_count)

    return {
        "table": target_table,
        "incoming": source_count,
        "inserted": inserted_count,
        "current": current_count,
        "historical": historical_count
    }


# ------------------------------------------------
# 5. PROCESS ALL FOUR RESOURCES
# ------------------------------------------------

results = []

for resource, config in RESOURCES.items():

    result = apply_scd2(
        config["stage"],
        config["target"]
    )

    results.append(result)


# ------------------------------------------------
# 6. FINAL SUMMARY
# ------------------------------------------------

print("\n======================================")
print("FHIR SCD TYPE 2 SUMMARY")
print("======================================")

for result in results:
    print(result)

print("\nSCD Type 2 processing completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
