CREATE TABLE [dbo].[fact_patient_condition] (
    [patient_condition_fact_key] BIGINT        IDENTITY NOT NULL,
    [condition_id]               VARCHAR (200) NOT NULL,
    [source_version_id]          VARCHAR (200) NULL,
    [patient_key]                BIGINT        NULL,
    [encounter_key]              BIGINT        NULL,
    [patient_id]                 VARCHAR (200) NULL,
    [encounter_id]               VARCHAR (200) NULL,
    [condition_code]             VARCHAR (500) NULL,
    [clinical_status]            VARCHAR (200) NULL,
    [verification_status]        VARCHAR (200) NULL,
    [onset_datetime]             DATETIME2 (6) NULL,
    [recorded_date]              DATE          NULL,
    [is_current]                 BIT           NULL,
    [valid_from]                 DATETIME2 (6) NULL,
    [valid_to]                   DATETIME2 (6) NULL,
    [gold_loaded_at]             DATETIME2 (6) NULL
);


GO