CREATE TABLE [audit].[gold_quality_results] (
    [quality_id]      BIGINT        IDENTITY NOT NULL,
    [table_name]      VARCHAR (200) NULL,
    [check_name]      VARCHAR (300) NULL,
    [total_records]   BIGINT        NULL,
    [failed_records]  BIGINT        NULL,
    [check_status]    VARCHAR (30)  NULL,
    [check_timestamp] DATETIME2 (6) NULL,
    [pipeline_run_id] VARCHAR (200) NULL
);


GO