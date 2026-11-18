CREATE TABLE [dbo].[dim_condition] (
    [condition_key]       BIGINT         IDENTITY NOT NULL,
    [condition_id]        VARCHAR (200)  NOT NULL,
    [source_version_id]   VARCHAR (200)  NULL,
    [patient_reference]   VARCHAR (500)  NULL,
    [encounter_reference] VARCHAR (500)  NULL,
    [clinical_status]     VARCHAR (200)  NULL,
    [verification_status] VARCHAR (200)  NULL,
    [category_code]       VARCHAR (200)  NULL,
    [condition_code]      VARCHAR (500)  NULL,
    [condition_display]   VARCHAR (1000) NULL,
    [code_system]         VARCHAR (1000) NULL,
    [onset_datetime]      DATETIME2 (6)  NULL,
    [recorded_date]       DATE           NULL,
    [valid_from]          DATETIME2 (6)  NULL,
    [valid_to]            DATETIME2 (6)  NULL,
    [is_current]          BIT            NULL,
    [record_hash]         VARCHAR (100)  NULL,
    [source_last_updated] DATETIME2 (6)  NULL,
    [gold_loaded_at]      DATETIME2 (6)  NULL
);


GO