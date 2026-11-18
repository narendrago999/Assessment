-- ============================================================
-- SYNAPX FHIR GOLD
-- SCRIPT 03: REPORTING VIEWS
-- ============================================================

-- 1. PATIENT CLINICAL SUMMARY

CREATE VIEW dbo.vw_patient_clinical_summary
AS
SELECT
    p.patient_key,
    p.patient_id,
    p.patient_name,
    p.gender,
    p.birth_date,
    p.active,

    COUNT(DISTINCT e.encounter_id) AS encounter_count,

    COUNT(DISTINCT c.condition_id) AS condition_count,

    COUNT(DISTINCT o.observation_id) AS observation_count

FROM dbo.dim_patient p

LEFT JOIN dbo.fact_encounter e
    ON e.patient_key = p.patient_key
    AND e.is_current = 1

LEFT JOIN dbo.fact_patient_condition c
    ON c.patient_key = p.patient_key
    AND c.is_current = 1

LEFT JOIN dbo.fact_observation o
    ON o.patient_key = p.patient_key
    AND o.is_current = 1

WHERE p.is_current = 1

GROUP BY
    p.patient_key,
    p.patient_id,
    p.patient_name,
    p.gender,
    p.birth_date,
    p.active;

GO