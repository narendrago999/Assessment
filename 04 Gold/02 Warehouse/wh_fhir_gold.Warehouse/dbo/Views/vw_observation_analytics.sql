-- 2. OBSERVATION ANALYTICS

CREATE VIEW dbo.vw_observation_analytics
AS
SELECT
    observation_code,
    observation_display,
    category_code,
    category_display,
    value_unit,
    COUNT_BIG(*) AS observation_count,
    AVG(value_quantity) AS average_value,
    MIN(value_quantity) AS minimum_value,
    MAX(value_quantity) AS maximum_value,
    COUNT(DISTINCT patient_id) AS distinct_patients

FROM dbo.fact_observation

WHERE is_current = 1

GROUP BY
    observation_code,
    observation_display,
    category_code,
    category_display,
    value_unit;

GO