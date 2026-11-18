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
# NOTEBOOK: nb_03_silver_transformation
# LAYER: SILVER - STANDARDIZATION AND CLEANING
# PLATFORM: MICROSOFT FABRIC / PYSPARK
# ================================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from datetime import datetime, timezone

# ------------------------------------------------
# 1. CONFIGURATION
# ------------------------------------------------

SOURCE_TABLES = {
    "Patient": "dbo.bronze_patient",
    "Encounter": "dbo.bronze_encounter",
    "Observation": "dbo.bronze_observation",
    "Condition": "dbo.bronze_condition"
}

TARGET_TABLES = {
    "Patient": "dbo.silver_stage_patient",
    "Encounter": "dbo.silver_stage_encounter",
    "Observation": "dbo.silver_stage_observation",
    "Condition": "dbo.silver_stage_condition"
}

EXECUTION_TIMESTAMP = datetime.now(
    timezone.utc
).isoformat()

print("FHIR Silver transformation started")
print("Execution timestamp:", EXECUTION_TIMESTAMP)


# ------------------------------------------------
# 2. HELPER FUNCTIONS
# ------------------------------------------------

def json_field(column_name, json_path):
    return F.get_json_object(
        F.col(column_name),
        json_path
    )


def parse_timestamp(column_name):
    return F.coalesce(
        F.to_timestamp(F.col(column_name)),
        F.to_timestamp(
            F.col(column_name),
            "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        )
    )


def base_dataframe(source_table):

    if not spark.catalog.tableExists(source_table):
        raise RuntimeError(
            f"Source table not found: {source_table}"
        )

    df = spark.table(source_table)

    required_columns = [
        "resource_id",
        "resource_type",
        "source_version_id",
        "source_last_updated",
        "extraction_timestamp",
        "api_url_or_params",
        "api_call_timestamp",
        "data_saved_timestamp",
        "run_id",
        "raw_resource_json",
        "raw_file_path"
    ]

    missing = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing:
        raise RuntimeError(
            f"{source_table} is missing columns: {missing}"
        )

    return df


def add_common_columns(df):

    return df.select(
        F.trim(F.col("resource_id")).alias("resource_id"),

        F.col("resource_type"),

        F.col("source_version_id"),

        parse_timestamp(
            "source_last_updated"
        ).alias("source_last_updated"),

        parse_timestamp(
            "extraction_timestamp"
        ).alias("extraction_timestamp"),

        F.col("api_url_or_params"),

        parse_timestamp(
            "api_call_timestamp"
        ).alias("api_call_timestamp"),

        parse_timestamp(
            "data_saved_timestamp"
        ).alias("data_saved_timestamp"),

        F.col("run_id"),

        F.col("raw_file_path"),

        F.col("raw_resource_json"),

        F.col("page_number"),

        F.col("entry_index")
    )


def extract_code_display(raw_json, path):
    return json_field(raw_json, path)


def extract_coding_code(raw_json, path):
    return json_field(raw_json, path)


# ------------------------------------------------
# 3. PATIENT TRANSFORMATION
# ------------------------------------------------

def transform_patient():

    source = base_dataframe(
        SOURCE_TABLES["Patient"]
    )

    df = add_common_columns(source)

    # Patient.name is an array.
    # Extract the first name's text, or construct it from
    # given and family names.

    given_schema = ArrayType(StringType())

    given_names = F.from_json(
        json_field("raw_resource_json", "$.name[0].given"),
        given_schema
    )

    given_name = F.concat_ws(
        " ",
        given_names
    )

    patient_name = F.coalesce(
        json_field("raw_resource_json", "$.name[0].text"),
        F.concat_ws(
            " ",
            given_name,
            json_field("raw_resource_json", "$.name[0].family")
        )
    )

    result = df.select(
        "*",

        json_field(
            "raw_resource_json",
            "$.active"
        ).cast("boolean").alias("active"),

        F.upper(
            json_field(
                "raw_resource_json",
                "$.gender"
            )
        ).alias("gender"),

        F.to_date(
            json_field(
                "raw_resource_json",
                "$.birthDate"
            )
        ).alias("birth_date"),

        patient_name.alias("patient_name"),

        json_field(
            "raw_resource_json",
            "$.name[0].family"
        ).alias("family_name"),

        json_field(
            "raw_resource_json",
            "$.name[0].use"
        ).alias("name_use"),

        json_field(
            "raw_resource_json",
            "$.identifier[0].system"
        ).alias("identifier_system"),

        json_field(
            "raw_resource_json",
            "$.identifier[0].value"
        ).alias("identifier_value"),

        json_field(
            "raw_resource_json",
            "$.managingOrganization.reference"
        ).alias("managing_organization_reference"),

        json_field(
            "raw_resource_json",
            "$.maritalStatus.text"
        ).alias("marital_status"),

        F.current_timestamp().alias("silver_transformed_at")
    )

    return result


# ------------------------------------------------
# 4. ENCOUNTER TRANSFORMATION
# ------------------------------------------------

def transform_encounter():

    source = base_dataframe(
        SOURCE_TABLES["Encounter"]
    )

    df = add_common_columns(source)

    result = df.select(
        "*",

        F.upper(
            json_field(
                "raw_resource_json",
                "$.status"
            )
        ).alias("status"),

        json_field(
            "raw_resource_json",
            "$.class.code"
        ).alias("encounter_class_code"),

        json_field(
            "raw_resource_json",
            "$.class.display"
        ).alias("encounter_class_display"),

        json_field(
            "raw_resource_json",
            "$.subject.reference"
        ).alias("patient_reference"),

        json_field(
            "raw_resource_json",
            "$.serviceProvider.reference"
        ).alias("service_provider_reference"),

        json_field(
            "raw_resource_json",
            "$.period.start"
        ).alias("period_start_raw"),

        json_field(
            "raw_resource_json",
            "$.period.end"
        ).alias("period_end_raw"),

        json_field(
            "raw_resource_json",
            "$.type[0].text"
        ).alias("encounter_type"),

        json_field(
            "raw_resource_json",
            "$.priority.text"
        ).alias("priority"),

        F.current_timestamp().alias("silver_transformed_at")
    )

    result = (
        result
        .withColumn(
            "period_start",
            F.to_timestamp("period_start_raw")
        )
        .withColumn(
            "period_end",
            F.to_timestamp("period_end_raw")
        )
        .drop("period_start_raw", "period_end_raw")
    )

    return result


# ------------------------------------------------
# 5. OBSERVATION TRANSFORMATION
# ------------------------------------------------

def transform_observation():

    source = base_dataframe(
        SOURCE_TABLES["Observation"]
    )

    df = add_common_columns(source)

    # FHIR Observation uses polymorphic value[x].
    # Different records can contain different value types.

    result = df.select(
        "*",

        F.upper(
            json_field(
                "raw_resource_json",
                "$.status"
            )
        ).alias("status"),

        json_field(
            "raw_resource_json",
            "$.category[0].coding[0].code"
        ).alias("category_code"),

        json_field(
            "raw_resource_json",
            "$.category[0].coding[0].display"
        ).alias("category_display"),

        json_field(
            "raw_resource_json",
            "$.code.coding[0].code"
        ).alias("observation_code"),

        json_field(
            "raw_resource_json",
            "$.code.coding[0].display"
        ).alias("observation_display"),

        json_field(
            "raw_resource_json",
            "$.code.coding[0].system"
        ).alias("code_system"),

        json_field(
            "raw_resource_json",
            "$.subject.reference"
        ).alias("patient_reference"),

        json_field(
            "raw_resource_json",
            "$.encounter.reference"
        ).alias("encounter_reference"),

        json_field(
            "raw_resource_json",
            "$.effectiveDateTime"
        ).alias("effective_datetime_raw"),

        json_field(
            "raw_resource_json",
            "$.valueQuantity.value"
        ).cast("double").alias("value_quantity"),

        json_field(
            "raw_resource_json",
            "$.valueQuantity.unit"
        ).alias("value_unit"),

        json_field(
            "raw_resource_json",
            "$.valueQuantity.code"
        ).alias("value_unit_code"),

        json_field(
            "raw_resource_json",
            "$.valueString"
        ).alias("value_string"),

        json_field(
            "raw_resource_json",
            "$.valueBoolean"
        ).cast("boolean").alias("value_boolean"),

        json_field(
            "raw_resource_json",
            "$.valueInteger"
        ).cast("integer").alias("value_integer"),

        json_field(
            "raw_resource_json",
            "$.valueCodeableConcept.text"
        ).alias("value_concept_text"),

        F.current_timestamp().alias("silver_transformed_at")
    )

    result = result.withColumn(
        "effective_datetime",
        F.to_timestamp("effective_datetime_raw")
    ).drop(
        "effective_datetime_raw"
    )

    # Unified value representation for analytics.
    result = result.withColumn(
        "observation_value",
        F.coalesce(
            F.col("value_string"),
            F.col("value_quantity").cast("string"),
            F.col("value_boolean").cast("string"),
            F.col("value_integer").cast("string"),
            F.col("value_concept_text")
        )
    )

    return result


# ------------------------------------------------
# 6. CONDITION TRANSFORMATION
# ------------------------------------------------

def transform_condition():

    source = base_dataframe(
        SOURCE_TABLES["Condition"]
    )

    df = add_common_columns(source)

    result = df.select(
        "*",

        json_field(
            "raw_resource_json",
            "$.clinicalStatus.coding[0].code"
        ).alias("clinical_status"),

        json_field(
            "raw_resource_json",
            "$.verificationStatus.coding[0].code"
        ).alias("verification_status"),

        json_field(
            "raw_resource_json",
            "$.category[0].coding[0].code"
        ).alias("category_code"),

        json_field(
            "raw_resource_json",
            "$.code.coding[0].code"
        ).alias("condition_code"),

        json_field(
            "raw_resource_json",
            "$.code.coding[0].display"
        ).alias("condition_display"),

        json_field(
            "raw_resource_json",
            "$.code.coding[0].system"
        ).alias("code_system"),

        json_field(
            "raw_resource_json",
            "$.subject.reference"
        ).alias("patient_reference"),

        json_field(
            "raw_resource_json",
            "$.encounter.reference"
        ).alias("encounter_reference"),

        json_field(
            "raw_resource_json",
            "$.onsetDateTime"
        ).alias("onset_datetime_raw"),

        json_field(
            "raw_resource_json",
            "$.recordedDate"
        ).alias("recorded_date_raw"),

        F.current_timestamp().alias("silver_transformed_at")
    )

    result = (
        result
        .withColumn(
            "onset_datetime",
            F.to_timestamp("onset_datetime_raw")
        )
        .withColumn(
            "recorded_date",
            F.to_date("recorded_date_raw")
        )
        .drop(
            "onset_datetime_raw",
            "recorded_date_raw"
        )
    )

    return result


# ------------------------------------------------
# 7. RESOURCE TRANSFORMATION MAPPING
# ------------------------------------------------

TRANSFORMERS = {
    "Patient": transform_patient,
    "Encounter": transform_encounter,
    "Observation": transform_observation,
    "Condition": transform_condition
}


# ------------------------------------------------
# 8. DEDUPLICATION
# ------------------------------------------------

def deduplicate_latest(df):

    # Keep the latest source version observed for each
    # resource ID. Prefer source lastUpdated, then
    # extraction timestamp, then stable lineage fields.

    window = (
        Window
        .partitionBy("resource_id")
        .orderBy(
            F.col("source_last_updated").desc_nulls_last(),
            F.col("extraction_timestamp").desc_nulls_last(),
            F.col("source_version_id").desc_nulls_last(),
            F.col("raw_file_path").desc_nulls_last(),
            F.col("page_number").desc_nulls_last(),
            F.col("entry_index").desc_nulls_last()
        )
    )

    return (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(window)
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )


# ------------------------------------------------
# 9. TRANSFORM AND WRITE STAGING TABLES
# ------------------------------------------------

summary = []

for resource, transformer in TRANSFORMERS.items():

    print("\nProcessing:", resource)

    transformed_df = transformer()

    # Required key validation.
    transformed_df = transformed_df.filter(
        F.col("resource_id").isNotNull()
        & (F.length(F.col("resource_id")) > 0)
    )

    deduplicated_df = deduplicate_latest(
        transformed_df
    )

    # Record hash is calculated from business fields,
    # excluding run IDs, file paths and processing timestamps.
    # This prevents ingestion metadata changes from creating
    # artificial SCD2 versions.

    business_columns = [
        column
        for column in deduplicated_df.columns
        if column not in [
            "run_id",
            "raw_file_path",
            "page_number",
            "entry_index",
            "extraction_timestamp",
            "api_call_timestamp",
            "data_saved_timestamp",
            "silver_transformed_at",
            "record_hash"
        ]
    ]

    hash_struct = F.struct(
        *[
            F.col(column)
            for column in business_columns
        ]
    )

    deduplicated_df = deduplicated_df.withColumn(
        "record_hash",
        F.sha2(
            F.to_json(hash_struct),
            256
        )
    )

    target_table = TARGET_TABLES[resource]

    (
        deduplicated_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    record_count = deduplicated_df.count()

    summary.append({
        "resource": resource,
        "stage_table": target_table,
        "records": record_count
    })

    print(
        f"{resource} completed: {record_count} records"
    )


# ------------------------------------------------
# 10. EXECUTION SUMMARY
# ------------------------------------------------

print("\n======================================")
print("SILVER TRANSFORMATION SUMMARY")
print("======================================")

for item in summary:
    print(
        item["resource"],
        "|",
        item["stage_table"],
        "|",
        item["records"]
    )

print("\nSilver staging transformation completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
