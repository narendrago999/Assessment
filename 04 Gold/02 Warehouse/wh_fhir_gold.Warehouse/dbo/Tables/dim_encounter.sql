CREATE TABLE [dbo].[dim_encounter] (
    [encounter_key]              BIGINT        IDENTITY NOT NULL,
    [encounter_id]               VARCHAR (200) NOT NULL,
    [source_version_id]          VARCHAR (200) NULL,
    [patient_reference]          VARCHAR (500) NULL,
    [encounter_status]           VARCHAR (100) NULL,
    [encounter_class_code]       VARCHAR (200) NULL,
    [encounter_class_display]    VARCHAR (500) NULL,
    [encounter_type]             VARCHAR (500) NULL,
    [priority]                   VARCHAR (200) NULL,
    [service_provider_reference] VARCHAR (500) NULL,
    [period_start]               DATETIME2 (6) NULL,
    [period_end]                 DATETIME2 (6) NULL,
    [valid_from]                 DATETIME2 (6) NULL,
    [valid_to]                   DATETIME2 (6) NULL,
    [is_current]                 BIT           NULL,
    [record_hash]                VARCHAR (100) NULL,
    [source_last_updated]        DATETIME2 (6) NULL,
    [gold_loaded_at]             DATETIME2 (6) NULL
);


GO