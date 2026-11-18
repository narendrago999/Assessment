CREATE PROCEDURE dbo.usp_load_gold
    @pipeline_name VARCHAR(200),
    @pipeline_run_id VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
        @rows_patient BIGINT = 0,
        @rows_encounter BIGINT = 0,
        @rows_condition BIGINT = 0,
        @rows_observation BIGINT = 0,
        @rows_fact_encounter BIGINT = 0,
        @rows_patient_condition BIGINT = 0;

    BEGIN TRY

        BEGIN TRANSACTION;

        /* Clear facts before dimensions */

        TRUNCATE TABLE dbo.fact_observation;
        TRUNCATE TABLE dbo.fact_encounter;
        TRUNCATE TABLE dbo.fact_patient_condition;

        TRUNCATE TABLE dbo.dim_condition;
        TRUNCATE TABLE dbo.dim_encounter;
        TRUNCATE TABLE dbo.dim_patient;


        /* 1. PATIENT DIMENSION */

        INSERT INTO dbo.dim_patient
        (
            patient_id,
            source_version_id,
            patient_name,
            family_name,
            gender,
            birth_date,
            active,
            identifier_system,
            identifier_value,
            managing_organization_reference,
            marital_status,
            valid_from,
            valid_to,
            is_current,
            record_hash,
            source_last_updated,
            gold_loaded_at
        )
        SELECT
            resource_id,
            source_version_id,
            patient_name,
            family_name,
            gender,
            birth_date,
            active,
            identifier_system,
            identifier_value,
            managing_organization_reference,
            marital_status,
            valid_from,
            valid_to,
            is_current,
            record_hash,
            source_last_updated,
            GETDATE()
        FROM [lh_fhir_silver].[dbo].[silver_patient];

        SET @rows_patient = @@ROWCOUNT;


        /* 2. ENCOUNTER DIMENSION */

        INSERT INTO dbo.dim_encounter
        (
            encounter_id,
            source_version_id,
            patient_reference,
            encounter_status,
            encounter_class_code,
            encounter_class_display,
            encounter_type,
            priority,
            service_provider_reference,
            period_start,
            period_end,
            valid_from,
            valid_to,
            is_current,
            record_hash,
            source_last_updated,
            gold_loaded_at
        )
        SELECT
            resource_id,
            source_version_id,
            patient_reference,
            status,
            encounter_class_code,
            encounter_class_display,
            encounter_type,
            priority,
            service_provider_reference,
            period_start,
            period_end,
            valid_from,
            valid_to,
            is_current,
            record_hash,
            source_last_updated,
            GETDATE()
        FROM [lh_fhir_silver].[dbo].[silver_encounter];

        SET @rows_encounter = @@ROWCOUNT;


        /* 3. CONDITION DIMENSION */

        INSERT INTO dbo.dim_condition
        (
            condition_id,
            source_version_id,
            patient_reference,
            encounter_reference,
            clinical_status,
            verification_status,
            category_code,
            condition_code,
            condition_display,
            code_system,
            onset_datetime,
            recorded_date,
            valid_from,
            valid_to,
            is_current,
            record_hash,
            source_last_updated,
            gold_loaded_at
        )
        SELECT
            resource_id,
            source_version_id,
            patient_reference,
            encounter_reference,
            clinical_status,
            verification_status,
            category_code,
            condition_code,
            condition_display,
            code_system,
            onset_datetime,
            recorded_date,
            valid_from,
            valid_to,
            is_current,
            record_hash,
            source_last_updated,
            GETDATE()
        FROM [lh_fhir_silver].[dbo].[silver_condition];

        SET @rows_condition = @@ROWCOUNT;


        /* 4. OBSERVATION FACT */

        INSERT INTO dbo.fact_observation
        (
            observation_id,
            source_version_id,
            patient_key,
            encounter_key,
            patient_id,
            encounter_id,
            observation_status,
            category_code,
            category_display,
            observation_code,
            observation_display,
            code_system,
            observation_value,
            value_quantity,
            value_unit,
            value_integer,
            value_boolean,
            effective_datetime,
            valid_from,
            valid_to,
            is_current,
            source_last_updated,
            gold_loaded_at
        )
        SELECT
            o.resource_id,
            o.source_version_id,
            p.patient_key,
            e.encounter_key,
            CASE
                WHEN o.patient_reference IS NULL THEN NULL
                WHEN CHARINDEX('/', o.patient_reference) > 0
                THEN RIGHT(
                    o.patient_reference,
                    CHARINDEX('/', REVERSE(o.patient_reference)) - 1
                )
                ELSE o.patient_reference
            END,
            CASE
                WHEN o.encounter_reference IS NULL THEN NULL
                WHEN CHARINDEX('/', o.encounter_reference) > 0
                THEN RIGHT(
                    o.encounter_reference,
                    CHARINDEX('/', REVERSE(o.encounter_reference)) - 1
                )
                ELSE o.encounter_reference
            END,
            o.status,
            o.category_code,
            o.category_display,
            o.observation_code,
            o.observation_display,
            o.code_system,
            o.observation_value,
            o.value_quantity,
            o.value_unit,
            o.value_integer,
            o.value_boolean,
            o.effective_datetime,
            o.valid_from,
            o.valid_to,
            o.is_current,
            o.source_last_updated,
            GETDATE()
        FROM [lh_fhir_silver].[dbo].[silver_observation] o
        LEFT JOIN dbo.dim_patient p
            ON p.patient_id =
                CASE
                    WHEN o.patient_reference IS NULL THEN NULL
                    WHEN CHARINDEX('/', o.patient_reference) > 0
                    THEN RIGHT(
                        o.patient_reference,
                        CHARINDEX('/', REVERSE(o.patient_reference)) - 1
                    )
                    ELSE o.patient_reference
                END
            AND p.is_current = 1
        LEFT JOIN dbo.dim_encounter e
            ON e.encounter_id =
                CASE
                    WHEN o.encounter_reference IS NULL THEN NULL
                    WHEN CHARINDEX('/', o.encounter_reference) > 0
                    THEN RIGHT(
                        o.encounter_reference,
                        CHARINDEX('/', REVERSE(o.encounter_reference)) - 1
                    )
                    ELSE o.encounter_reference
                END
            AND e.is_current = 1;

        SET @rows_observation = @@ROWCOUNT;


        /* 5. ENCOUNTER FACT */

        INSERT INTO dbo.fact_encounter
        (
            encounter_id,
            source_version_id,
            encounter_key,
            patient_key,
            patient_id,
            encounter_status,
            encounter_class_code,
            period_start,
            period_end,
            duration_minutes,
            is_completed,
            is_current,
            valid_from,
            valid_to,
            gold_loaded_at
        )
        SELECT
            s.resource_id,
            s.source_version_id,
            d.encounter_key,
            p.patient_key,
            CASE
                WHEN s.patient_reference IS NULL THEN NULL
                WHEN CHARINDEX('/', s.patient_reference) > 0
                THEN RIGHT(
                    s.patient_reference,
                    CHARINDEX('/', REVERSE(s.patient_reference)) - 1
                )
                ELSE s.patient_reference
            END,
            s.status,
            s.encounter_class_code,
            s.period_start,
            s.period_end,
            CASE
                WHEN s.period_start IS NOT NULL
                 AND s.period_end IS NOT NULL
                 AND s.period_end >= s.period_start
                THEN DATEDIFF(SECOND, s.period_start, s.period_end) / 60.0
                ELSE NULL
            END,
            CASE
                WHEN UPPER(s.status) = 'FINISHED' THEN 1
                ELSE 0
            END,
            s.is_current,
            s.valid_from,
            s.valid_to,
            GETDATE()
        FROM [lh_fhir_silver].[dbo].[silver_encounter] s
        LEFT JOIN dbo.dim_encounter d
            ON d.encounter_id = s.resource_id
            AND (
                d.source_version_id = s.source_version_id
                OR (
                    d.source_version_id IS NULL
                    AND s.source_version_id IS NULL
                )
            )
            AND (
                d.valid_from = s.valid_from
                OR (
                    d.valid_from IS NULL
                    AND s.valid_from IS NULL
                )
            )
        LEFT JOIN dbo.dim_patient p
            ON p.patient_id =
                CASE
                    WHEN s.patient_reference IS NULL THEN NULL
                    WHEN CHARINDEX('/', s.patient_reference) > 0
                    THEN RIGHT(
                        s.patient_reference,
                        CHARINDEX('/', REVERSE(s.patient_reference)) - 1
                    )
                    ELSE s.patient_reference
                END
            AND p.is_current = 1;

        SET @rows_fact_encounter = @@ROWCOUNT;


        /* 6. PATIENT-CONDITION FACT */

        INSERT INTO dbo.fact_patient_condition
        (
            condition_id,
            source_version_id,
            patient_key,
            encounter_key,
            patient_id,
            encounter_id,
            condition_code,
            clinical_status,
            verification_status,
            onset_datetime,
            recorded_date,
            is_current,
            valid_from,
            valid_to,
            gold_loaded_at
        )
        SELECT
            c.resource_id,
            c.source_version_id,
            p.patient_key,
            e.encounter_key,
            CASE
                WHEN c.patient_reference IS NULL THEN NULL
                WHEN CHARINDEX('/', c.patient_reference) > 0
                THEN RIGHT(
                    c.patient_reference,
                    CHARINDEX('/', REVERSE(c.patient_reference)) - 1
                )
                ELSE c.patient_reference
            END,
            CASE
                WHEN c.encounter_reference IS NULL THEN NULL
                WHEN CHARINDEX('/', c.encounter_reference) > 0
                THEN RIGHT(
                    c.encounter_reference,
                    CHARINDEX('/', REVERSE(c.encounter_reference)) - 1
                )
                ELSE c.encounter_reference
            END,
            c.condition_code,
            c.clinical_status,
            c.verification_status,
            c.onset_datetime,
            c.recorded_date,
            c.is_current,
            c.valid_from,
            c.valid_to,
            GETDATE()
        FROM [lh_fhir_silver].[dbo].[silver_condition] c
        LEFT JOIN dbo.dim_patient p
            ON p.patient_id =
                CASE
                    WHEN c.patient_reference IS NULL THEN NULL
                    WHEN CHARINDEX('/', c.patient_reference) > 0
                    THEN RIGHT(
                        c.patient_reference,
                        CHARINDEX('/', REVERSE(c.patient_reference)) - 1
                    )
                    ELSE c.patient_reference
                END
            AND p.is_current = 1
        LEFT JOIN dbo.dim_encounter e
            ON e.encounter_id =
                CASE
                    WHEN c.encounter_reference IS NULL THEN NULL
                    WHEN CHARINDEX('/', c.encounter_reference) > 0
                    THEN RIGHT(
                        c.encounter_reference,
                        CHARINDEX('/', REVERSE(c.encounter_reference)) - 1
                    )
                    ELSE c.encounter_reference
                END
            AND e.is_current = 1;

        SET @rows_patient_condition = @@ROWCOUNT;


        /* 7. WRITE TABLE-LEVEL AUDIT RECORDS */

        INSERT INTO audit.gold_load_log
        (
            pipeline_name,
            table_name,
            load_type,
            rows_loaded,
            load_status,
            load_timestamp,
            error_message,
            pipeline_run_id
        )
        VALUES
        (@pipeline_name, 'dim_patient', 'FULL', @rows_patient, 'SUCCESS', GETDATE(), NULL, @pipeline_run_id),
        (@pipeline_name, 'dim_encounter', 'FULL', @rows_encounter, 'SUCCESS', GETDATE(), NULL, @pipeline_run_id),
        (@pipeline_name, 'dim_condition', 'FULL', @rows_condition, 'SUCCESS', GETDATE(), NULL, @pipeline_run_id),
        (@pipeline_name, 'fact_observation', 'FULL', @rows_observation, 'SUCCESS', GETDATE(), NULL, @pipeline_run_id),
        (@pipeline_name, 'fact_encounter', 'FULL', @rows_fact_encounter, 'SUCCESS', GETDATE(), NULL, @pipeline_run_id),
        (@pipeline_name, 'fact_patient_condition', 'FULL', @rows_patient_condition, 'SUCCESS', GETDATE(), NULL, @pipeline_run_id);

        COMMIT TRANSACTION;


        /* 8. UPDATE PARENT AUDIT */

        UPDATE audit.gold_load_log
        SET
            load_status = 'LOADED',
            rows_loaded =
                @rows_patient
                + @rows_encounter
                + @rows_condition
                + @rows_observation
                + @rows_fact_encounter
                + @rows_patient_condition,
            load_timestamp = GETDATE()
        WHERE pipeline_run_id = @pipeline_run_id
          AND table_name = 'ALL';

        SELECT
            @rows_patient AS dim_patient_rows,
            @rows_encounter AS dim_encounter_rows,
            @rows_condition AS dim_condition_rows,
            @rows_observation AS fact_observation_rows,
            @rows_fact_encounter AS fact_encounter_rows,
            @rows_patient_condition AS fact_patient_condition_rows;

    END TRY

    BEGIN CATCH

        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        UPDATE audit.gold_load_log
        SET
            load_status = 'FAILED',
            error_message = ERROR_MESSAGE(),
            load_timestamp = GETDATE()
        WHERE pipeline_run_id = @pipeline_run_id
          AND table_name = 'ALL';

        INSERT INTO audit.gold_load_log
        (
            pipeline_name,
            table_name,
            load_type,
            rows_loaded,
            load_status,
            load_timestamp,
            error_message,
            pipeline_run_id
        )
        VALUES
        (
            @pipeline_name,
            'ALL',
            'FULL',
            0,
            'FAILED',
            GETDATE(),
            ERROR_MESSAGE(),
            @pipeline_run_id
        );

        THROW;

    END CATCH;
END;

GO