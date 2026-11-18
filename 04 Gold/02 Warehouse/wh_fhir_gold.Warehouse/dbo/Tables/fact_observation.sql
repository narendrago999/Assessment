CREATE TABLE [dbo].[fact_observation] (
    [observation_fact_key] BIGINT         IDENTITY NOT NULL,
    [observation_id]       VARCHAR (200)  NOT NULL,
    [source_version_id]    VARCHAR (200)  NULL,
    [patient_key]          BIGINT         NULL,
    [encounter_key]        BIGINT         NULL,
    [patient_id]           VARCHAR (200)  NULL,
    [encounter_id]         VARCHAR (200)  NULL,
    [observation_status]   VARCHAR (100)  NULL,
    [category_code]        VARCHAR (200)  NULL,
    [category_display]     VARCHAR (500)  NULL,
    [observation_code]     VARCHAR (500)  NULL,
    [observation_display]  VARCHAR (1000) NULL,
    [code_system]          VARCHAR (1000) NULL,
    [observation_value]    VARCHAR (4000) NULL,
    [value_quantity]       FLOAT (53)     NULL,
    [value_unit]           VARCHAR (200)  NULL,
    [value_integer]        INT            NULL,
    [value_boolean]        BIT            NULL,
    [effective_datetime]   DATETIME2 (6)  NULL,
    [valid_from]           DATETIME2 (6)  NULL,
    [valid_to]             DATETIME2 (6)  NULL,
    [is_current]           BIT            NULL,
    [source_last_updated]  DATETIME2 (6)  NULL,
    [gold_loaded_at]       DATETIME2 (6)  NULL
);


GO