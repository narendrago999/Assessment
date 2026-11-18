CREATE PROCEDURE audit.usp_validate_gold
    @pipeline_run_id VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @failed_checks BIGINT = 0;

    /* 1. TABLE POPULATION CHECKS */

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT 'dim_patient', 'Table has records',
        COUNT_BIG(*),
        CASE WHEN COUNT_BIG(*) = 0 THEN 1 ELSE 0 END,
        CASE WHEN COUNT_BIG(*) = 0 THEN 'FAILED' ELSE 'PASSED' END,
        GETDATE(), @pipeline_run_id
    FROM dbo.dim_patient;

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT 'dim_encounter', 'Table has records',
        COUNT_BIG(*),
        CASE WHEN COUNT_BIG(*) = 0 THEN 1 ELSE 0 END,
        CASE WHEN COUNT_BIG(*) = 0 THEN 'FAILED' ELSE 'PASSED' END,
        GETDATE(), @pipeline_run_id
    FROM dbo.dim_encounter;

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT 'dim_condition', 'Table has records',
        COUNT_BIG(*),
        CASE WHEN COUNT_BIG(*) = 0 THEN 1 ELSE 0 END,
        CASE WHEN COUNT_BIG(*) = 0 THEN 'FAILED' ELSE 'PASSED' END,
        GETDATE(), @pipeline_run_id
    FROM dbo.dim_condition;

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT 'fact_observation', 'Table has records',
        COUNT_BIG(*),
        CASE WHEN COUNT_BIG(*) = 0 THEN 1 ELSE 0 END,
        CASE WHEN COUNT_BIG(*) = 0 THEN 'FAILED' ELSE 'PASSED' END,
        GETDATE(), @pipeline_run_id
    FROM dbo.fact_observation;

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT 'fact_encounter', 'Table has records',
        COUNT_BIG(*),
        CASE WHEN COUNT_BIG(*) = 0 THEN 1 ELSE 0 END,
        CASE WHEN COUNT_BIG(*) = 0 THEN 'FAILED' ELSE 'PASSED' END,
        GETDATE(), @pipeline_run_id
    FROM dbo.fact_encounter;

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT 'fact_patient_condition', 'Table has records',
        COUNT_BIG(*),
        CASE WHEN COUNT_BIG(*) = 0 THEN 1 ELSE 0 END,
        CASE WHEN COUNT_BIG(*) = 0 THEN 'FAILED' ELSE 'PASSED' END,
        GETDATE(), @pipeline_run_id
    FROM dbo.fact_patient_condition;


    /* 2. DUPLICATE CURRENT PATIENT IDs */

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT
        'dim_patient',
        'Duplicate current patient IDs',
        COUNT_BIG(*),
        (
            SELECT COUNT_BIG(*)
            FROM
            (
                SELECT patient_id
                FROM dbo.dim_patient
                WHERE is_current = 1
                GROUP BY patient_id
                HAVING COUNT_BIG(*) > 1
            ) d
        ),
        CASE
            WHEN (
                SELECT COUNT_BIG(*)
                FROM
                (
                    SELECT patient_id
                    FROM dbo.dim_patient
                    WHERE is_current = 1
                    GROUP BY patient_id
                    HAVING COUNT_BIG(*) > 1
                ) d
            ) = 0 THEN 'PASSED'
            ELSE 'FAILED'
        END,
        GETDATE(),
        @pipeline_run_id
    FROM dbo.dim_patient;


    /* 3. DUPLICATE CURRENT ENCOUNTER IDs */

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT
        'dim_encounter',
        'Duplicate current encounter IDs',
        COUNT_BIG(*),
        (
            SELECT COUNT_BIG(*)
            FROM
            (
                SELECT encounter_id
                FROM dbo.dim_encounter
                WHERE is_current = 1
                GROUP BY encounter_id
                HAVING COUNT_BIG(*) > 1
            ) d
        ),
        CASE
            WHEN (
                SELECT COUNT_BIG(*)
                FROM
                (
                    SELECT encounter_id
                    FROM dbo.dim_encounter
                    WHERE is_current = 1
                    GROUP BY encounter_id
                    HAVING COUNT_BIG(*) > 1
                ) d
            ) = 0 THEN 'PASSED'
            ELSE 'FAILED'
        END,
        GETDATE(),
        @pipeline_run_id
    FROM dbo.dim_encounter;


    /* 4. DUPLICATE CURRENT CONDITION IDs */

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT
        'dim_condition',
        'Duplicate current condition IDs',
        COUNT_BIG(*),
        (
            SELECT COUNT_BIG(*)
            FROM
            (
                SELECT condition_id
                FROM dbo.dim_condition
                WHERE is_current = 1
                GROUP BY condition_id
                HAVING COUNT_BIG(*) > 1
            ) d
        ),
        CASE
            WHEN (
                SELECT COUNT_BIG(*)
                FROM
                (
                    SELECT condition_id
                    FROM dbo.dim_condition
                    WHERE is_current = 1
                    GROUP BY condition_id
                    HAVING COUNT_BIG(*) > 1
                ) d
            ) = 0 THEN 'PASSED'
            ELSE 'FAILED'
        END,
        GETDATE(),
        @pipeline_run_id
    FROM dbo.dim_condition;


    /* 5. UNRESOLVED PATIENT KEYS */

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT
        'fact_observation',
        'Unresolved patient dimension keys',
        COUNT_BIG(*),
        COALESCE(SUM(
            CASE WHEN patient_id IS NOT NULL AND patient_key IS NULL
                 THEN 1 ELSE 0 END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE WHEN patient_id IS NOT NULL AND patient_key IS NULL
                     THEN 1 ELSE 0 END
            ), 0) = 0 THEN 'PASSED'
            ELSE 'FAILED'
        END,
        GETDATE(),
        @pipeline_run_id
    FROM dbo.fact_observation;

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT
        'fact_encounter',
        'Unresolved patient dimension keys',
        COUNT_BIG(*),
        COALESCE(SUM(
            CASE WHEN patient_id IS NOT NULL AND patient_key IS NULL
                 THEN 1 ELSE 0 END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE WHEN patient_id IS NOT NULL AND patient_key IS NULL
                     THEN 1 ELSE 0 END
            ), 0) = 0 THEN 'PASSED'
            ELSE 'FAILED'
        END,
        GETDATE(),
        @pipeline_run_id
    FROM dbo.fact_encounter;

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT
        'fact_patient_condition',
        'Unresolved patient dimension keys',
        COUNT_BIG(*),
        COALESCE(SUM(
            CASE WHEN patient_id IS NOT NULL AND patient_key IS NULL
                 THEN 1 ELSE 0 END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE WHEN patient_id IS NOT NULL AND patient_key IS NULL
                     THEN 1 ELSE 0 END
            ), 0) = 0 THEN 'PASSED'
            ELSE 'FAILED'
        END,
        GETDATE(),
        @pipeline_run_id
    FROM dbo.fact_patient_condition;


    /* 6. UNRESOLVED ENCOUNTER KEYS */

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT
        'fact_observation',
        'Unresolved encounter dimension keys',
        COUNT_BIG(*),
        COALESCE(SUM(
            CASE WHEN encounter_id IS NOT NULL AND encounter_key IS NULL
                 THEN 1 ELSE 0 END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE WHEN encounter_id IS NOT NULL AND encounter_key IS NULL
                     THEN 1 ELSE 0 END
            ), 0) = 0 THEN 'PASSED'
            ELSE 'FAILED'
        END,
        GETDATE(),
        @pipeline_run_id
    FROM dbo.fact_observation;

    INSERT INTO audit.gold_quality_results
    (
        table_name, check_name, total_records,
        failed_records, check_status, check_timestamp,
        pipeline_run_id
    )
    SELECT
        'fact_patient_condition',
        'Unresolved encounter dimension keys',
        COUNT_BIG(*),
        COALESCE(SUM(
            CASE WHEN encounter_id IS NOT NULL AND encounter_key IS NULL
                 THEN 1 ELSE 0 END
        ), 0),
        CASE
            WHEN COALESCE(SUM(
                CASE WHEN encounter_id IS NOT NULL AND encounter_key IS NULL
                     THEN 1 ELSE 0 END
            ), 0) = 0 THEN 'PASSED'
            ELSE 'FAILED'
        END,
        GETDATE(),
        @pipeline_run_id
    FROM dbo.fact_patient_condition;


    /* 7. COUNT FAILED CHECKS FOR THIS RUN */

    SELECT @failed_checks = COUNT_BIG(*)
    FROM audit.gold_quality_results
    WHERE pipeline_run_id = @pipeline_run_id
      AND check_status = 'FAILED';


    /* 8. UPDATE PARENT AUDIT */

    UPDATE audit.gold_load_log
    SET
        load_status =
            CASE WHEN @failed_checks = 0
                 THEN 'SUCCESS'
                 ELSE 'QUALITY_FAILED' END,
        load_timestamp = GETDATE(),
        error_message =
            CASE WHEN @failed_checks = 0
                 THEN NULL
                 ELSE 'Gold quality validation failed. Review audit.gold_quality_results.'
            END
    WHERE pipeline_run_id = @pipeline_run_id
      AND table_name = 'ALL';


    SELECT
        @pipeline_run_id AS pipeline_run_id,
        @failed_checks AS failed_checks,
        CASE WHEN @failed_checks = 0
             THEN 'SUCCESS'
             ELSE 'QUALITY_FAILED' END AS validation_status;

    IF @failed_checks > 0
    BEGIN
        THROW 51001,
            'Gold quality validation failed. Review audit.gold_quality_results.',
            1;
    END;
END;

GO