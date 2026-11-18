CREATE TABLE [audit].[gold_load_log] (
    [load_id]         BIGINT         IDENTITY NOT NULL,
    [pipeline_name]   VARCHAR (200)  NULL,
    [table_name]      VARCHAR (200)  NULL,
    [load_type]       VARCHAR (50)   NULL,
    [rows_loaded]     BIGINT         NULL,
    [load_status]     VARCHAR (50)   NULL,
    [load_timestamp]  DATETIME2 (6)  NULL,
    [error_message]   VARCHAR (4000) NULL,
    [pipeline_run_id] VARCHAR (200)  NULL
);


GO