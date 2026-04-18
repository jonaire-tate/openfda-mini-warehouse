-- 02_warehouse.sql
-- Phase 2: reads raw_adverse_events and builds warehouse_adverse_events.
-- Flattens drug and reaction arrays into one row per
-- report-drug-reaction combination, filtered to only the target narcolepsy
-- drugs, with dates and drug names cleaned.

INSTALL json;
LOAD json;

DROP TABLE IF EXISTS warehouse_adverse_events;


-- ---------- Main transformation ----------

CREATE TABLE warehouse_adverse_events AS
WITH extracted AS (
    SELECT
        json_extract_string(record_json, '$.safetyreportid') AS safety_report_id,
        json_extract_string(record_json, '$.receivedate') AS receive_date_raw,
        json_extract_string(record_json, '$.primarysourcecountry') AS source_country,
        json_extract_string(record_json, '$.serious') AS serious_code,
        json_extract_string(record_json, '$.patient.patientonsetage') AS patient_age_raw,
        json_extract_string(record_json, '$.patient.patientsex') AS patient_sex_code,
        json_extract(record_json, '$.patient.drug') AS drugs_json,
        json_extract(record_json, '$.patient.reaction') AS reactions_json,
        loaded_at
    FROM raw_adverse_events
),
flattened_drugs AS (
    -- UNNEST the drug array. One report with 3 drugs becomes 3 rows.
    SELECT
        safety_report_id,
        receive_date_raw,
        source_country,
        serious_code,
        patient_age_raw,
        patient_sex_code,
        reactions_json,
        loaded_at,
        UNNEST(json_extract(drugs_json, '$[*]')) AS drug_json
    FROM extracted
),
-- Each raw report lists every medication the patient was on, not just the
-- narcolepsy drug that matched the API search. Filter down to target drugs.
-- Also strip trailing punctuation so "XYREM." and "XYREM" merge.
target_drugs_only AS (
    SELECT *,
        UPPER(TRIM(BOTH '. ' FROM
            SPLIT_PART(json_extract_string(drug_json, '$.medicinalproduct'), ' ', 1)
        )) AS drug_name_clean
    FROM flattened_drugs
    WHERE UPPER(TRIM(BOTH '. ' FROM
            SPLIT_PART(json_extract_string(drug_json, '$.medicinalproduct'), ' ', 1)
         )) IN ('XYREM', 'XYWAV', 'WAKIX', 'SUNOSI')
),
flattened_reactions AS (
    SELECT
        safety_report_id,
        receive_date_raw,
        source_country,
        serious_code,
        patient_age_raw,
        patient_sex_code,
        drug_json,
        drug_name_clean,
        loaded_at,
        UNNEST(json_extract(reactions_json, '$[*]')) AS reaction_json
    FROM target_drugs_only
)
SELECT
    safety_report_id,

    -- FDA dates come in as YYYYMMDD strings with no separators.
    -- try_strptime returns NULL on bad input instead of crashing.
    TRY_CAST(try_strptime(receive_date_raw, '%Y%m%d') AS DATE) AS receive_date,
    EXTRACT(YEAR FROM TRY_CAST(try_strptime(receive_date_raw, '%Y%m%d') AS DATE)) AS receive_year,

    source_country,

    -- Decode FDA seriousness code
    CASE serious_code
        WHEN '1' THEN 'Serious'
        WHEN '2' THEN 'Non-serious'
        ELSE 'Unknown'
    END AS seriousness,

    TRY_CAST(patient_age_raw AS INTEGER) AS patient_age,

    -- Decode FDA sex code
    CASE patient_sex_code
        WHEN '1' THEN 'Male'
        WHEN '2' THEN 'Female'
        ELSE 'Unknown'
    END AS patient_sex,

    drug_name_clean AS drug_name,
    json_extract_string(drug_json, '$.drugindication') AS drug_indication,
    json_extract_string(drug_json, '$.openfda.generic_name[0]') AS generic_name,
    json_extract_string(drug_json, '$.openfda.manufacturer_name[0]') AS manufacturer_name,

    -- MedDRA preferred term for the reaction
    json_extract_string(reaction_json, '$.reactionmeddrapt') AS reaction_term,

    loaded_at
FROM flattened_reactions
WHERE safety_report_id IS NOT NULL;


-- ---------- Sanity checks ----------

-- Expect a few thousand rows now, not 98k
SELECT COUNT(*) AS total_rows FROM warehouse_adverse_events;

-- Expect exactly 4 unique drugs
SELECT
    COUNT(DISTINCT safety_report_id) AS unique_reports,
    COUNT(DISTINCT drug_name) AS unique_drugs,
    COUNT(DISTINCT reaction_term) AS unique_reactions
FROM warehouse_adverse_events;

-- Should show all 4 target drugs
SELECT drug_name, COUNT(*) AS row_count
FROM warehouse_adverse_events
GROUP BY drug_name
ORDER BY row_count DESC;

-- Dates should fall in roughly 2000–2025
SELECT
    MIN(receive_date) AS earliest_report,
    MAX(receive_date) AS latest_report
FROM warehouse_adverse_events;