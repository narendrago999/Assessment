CREATE PROCEDURE audit.usp_run_gold
    @pipeline_name VARCHAR(200),
    @pipeline_run_id VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        /* 1. Start audit */
        EXEC audit.usp_start_gold_load
            @pipeline_name = @pipeline_name,
            @pipeline_run_id = @pipeline_run_id;

        /* 2. Load Gold dimensions and facts */
        EXEC dbo.usp_load_gold
            @pipeline_name = @pipeline_name,
            @pipeline_run_id = @pipeline_run_id;

        /* 3. Validate Gold */
        EXEC audit.usp_validate_gold
            @pipeline_run_id = @pipeline_run_id;

        SELECT
            @pipeline_name AS pipeline_name,
            @pipeline_run_id AS pipeline_run_id,
            'SUCCESS' AS execution_status;

    END TRY

    BEGIN CATCH

        UPDATE audit.gold_load_log
        SET
            load_status = 'FAILED',
            error_message = ERROR_MESSAGE(),
            load_timestamp = GETDATE()
        WHERE pipeline_run_id = @pipeline_run_id
          AND table_name = 'ALL';

        THROW;

    END CATCH;
END;

GO