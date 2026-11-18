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


# ==================================================
# SYNAPX FHIR GOLD
# NOTEBOOK: nb_06_gold_source_validation
# PURPOSE: INSPECT SILVER SOURCE TABLES
# ==================================================

from pyspark.sql import functions as F

TABLES = {
    "Patient": "dbo.silver_patient",
    "Encounter": "dbo.silver_encounter",
    "Observation": "dbo.silver_observation",
    "Condition": "dbo.silver_condition"
}

print("=" * 65)
print("GOLD SOURCE VALIDATION")
print("=" * 65)

for resource, table_name in TABLES.items():

    print("\nRESOURCE:", resource)
    print("TABLE:", table_name)

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Missing Silver table: {table_name}"
        )

    df = spark.table(table_name)

    print("TOTAL ROWS:", df.count())

    print("\nSCHEMA:")
    df.printSchema()

    print("\nSAMPLE RECORDS:")
    display(df.limit(3))

    print("\nCURRENT RECORDS:")
    print(
        df.filter(
            F.col("is_current") == True
        ).count()
    )

print("\n================================")
print("GOLD SOURCE VALIDATION COMPLETED")
print("================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
