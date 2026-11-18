CREATE PROCEDURE audit.usp_start_gold_load
    @pipeline_name VARCHAR(200),
    @pipeline_run_id VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

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
        'RUNNING',
        GETDATE(),
        NULL,
        @pipeline_run_id
    );

    SELECT
        @pipeline_run_id AS pipeline_run_id,
        'RUNNING' AS load_status;
END;

GO