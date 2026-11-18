CREATE TABLE [dbo].[dim_patient] (
    [patient_key]                     BIGINT         IDENTITY NOT NULL,
    [patient_id]                      VARCHAR (200)  NOT NULL,
    [source_version_id]               VARCHAR (200)  NULL,
    [patient_name]                    VARCHAR (1000) NULL,
    [family_name]                     VARCHAR (500)  NULL,
    [gender]                          VARCHAR (50)   NULL,
    [birth_date]                      DATE           NULL,
    [active]                          BIT            NULL,
    [identifier_system]               VARCHAR (1000) NULL,
    [identifier_value]                VARCHAR (500)  NULL,
    [managing_organization_reference] VARCHAR (500)  NULL,
    [marital_status]                  VARCHAR (200)  NULL,
    [valid_from]                      DATETIME2 (6)  NULL,
    [valid_to]                        DATETIME2 (6)  NULL,
    [is_current]                      BIT            NULL,
    [record_hash]                     VARCHAR (100)  NULL,
    [source_last_updated]             DATETIME2 (6)  NULL,
    [gold_loaded_at]                  DATETIME2 (6)  NULL
);


GO