CREATE TABLE [dbo].[fact_encounter] (
    [encounter_fact_key]   BIGINT        IDENTITY NOT NULL,
    [encounter_id]         VARCHAR (200) NOT NULL,
    [source_version_id]    VARCHAR (200) NULL,
    [encounter_key]        BIGINT        NULL,
    [patient_key]          BIGINT        NULL,
    [patient_id]           VARCHAR (200) NULL,
    [encounter_status]     VARCHAR (100) NULL,
    [encounter_class_code] VARCHAR (200) NULL,
    [period_start]         DATETIME2 (6) NULL,
    [period_end]           DATETIME2 (6) NULL,
    [duration_minutes]     FLOAT (53)    NULL,
    [is_completed]         BIT           NULL,
    [is_current]           BIT           NULL,
    [valid_from]           DATETIME2 (6) NULL,
    [valid_to]             DATETIME2 (6) NULL,
    [gold_loaded_at]       DATETIME2 (6) NULL
);


GO