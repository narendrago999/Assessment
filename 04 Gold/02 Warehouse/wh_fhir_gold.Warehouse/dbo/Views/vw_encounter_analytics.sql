-- 3. ENCOUNTER ANALYTICS

CREATE VIEW dbo.vw_encounter_analytics
AS
SELECT
    encounter_status,
    encounter_class_code,
    COUNT_BIG(*) AS encounter_count,
    COUNT(DISTINCT patient_id) AS distinct_patients,
    AVG(duration_minutes) AS average_duration_minutes,
    MIN(period_start) AS earliest_encounter,
    MAX(period_start) AS latest_encounter

FROM dbo.fact_encounter

WHERE is_current = 1

GROUP BY
    encounter_status,
    encounter_class_code;

GO