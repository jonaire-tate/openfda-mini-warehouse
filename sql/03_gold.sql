-- 03_gold.sql
-- Phase 3: builds the gold layer from warehouse_adverse_events.
-- Splits the flat warehouse into three normalized tables plus an
-- analysis view that joins them back into an ML-ready flat table.

DROP VIEW IF EXISTS gold_analysis_view;
DROP TABLE IF EXISTS gold_reactions;
DROP TABLE IF EXISTS gold_reports;
DROP TABLE IF EXISTS gold_drugs;


-- ---------- gold_drugs (anchor table, stable data) ----------
-- One row per unique drug. drug_id is a surrogate key so the join tables
-- don't have to repeat long drug name strings.

CREATE TABLE gold_drugs (
    drug_id INTEGER PRIMARY KEY,
    drug_name VARCHAR NOT NULL UNIQUE,
    generic_name VARCHAR,
    manufacturer_name VARCHAR
);

INSERT INTO gold_drugs (drug_id, drug_name, generic_name, manufacturer_name)
SELECT
    ROW_NUMBER() OVER (ORDER BY drug_name) AS drug_id,
    drug_name,
    -- Use MAX to pick one value when the warehouse has slight variations
    MAX(generic_name) AS generic_name,
    MAX(manufacturer_name) AS manufacturer_name
FROM warehouse_adverse_events
GROUP BY drug_name;


-- ---------- gold_reports (time-sensitive metadata) ----------
-- One row per unique safety report. The report itself is the natural key
-- (FDA assigns it) so no surrogate needed here.

CREATE TABLE gold_reports (
    safety_report_id VARCHAR PRIMARY KEY,
    receive_date DATE,
    receive_year INTEGER,
    source_country VARCHAR,
    seriousness VARCHAR,
    patient_age INTEGER,
    patient_sex VARCHAR
);

INSERT INTO gold_reports
SELECT DISTINCT
    safety_report_id,
    receive_date,
    receive_year,
    source_country,
    seriousness,
    patient_age,
    patient_sex
FROM warehouse_adverse_events;


-- ---------- gold_reactions (fact table, one row per reaction event) ----------
-- Many-to-many bridge: each row ties one report to one drug to one reaction.
-- Uses foreign keys back to gold_drugs and gold_reports.

CREATE TABLE gold_reactions (
    reaction_id INTEGER PRIMARY KEY,
    safety_report_id VARCHAR NOT NULL,
    drug_id INTEGER NOT NULL,
    reaction_term VARCHAR,
    drug_indication VARCHAR,
    FOREIGN KEY (safety_report_id) REFERENCES gold_reports(safety_report_id),
    FOREIGN KEY (drug_id) REFERENCES gold_drugs(drug_id)
);

INSERT INTO gold_reactions (reaction_id, safety_report_id, drug_id, reaction_term, drug_indication)
SELECT
    ROW_NUMBER() OVER (ORDER BY w.safety_report_id, w.drug_name) AS reaction_id,
    w.safety_report_id,
    d.drug_id,
    w.reaction_term,
    w.drug_indication
FROM warehouse_adverse_events w
JOIN gold_drugs d ON w.drug_name = d.drug_name;


-- ---------- gold_analysis_view (ML-ready flat view) ----------
-- Joins all three gold tables back into a single flat result. This is the
-- pandas entry point: con.execute("SELECT * FROM gold_analysis_view").df()

CREATE VIEW gold_analysis_view AS
SELECT
    rx.reaction_id,
    rpt.safety_report_id,
    rpt.receive_date,
    rpt.receive_year,
    rpt.source_country,
    rpt.seriousness,
    rpt.patient_age,
    rpt.patient_sex,
    d.drug_id,
    d.drug_name,
    d.generic_name,
    d.manufacturer_name,
    rx.drug_indication,
    rx.reaction_term
FROM gold_reactions rx
JOIN gold_reports rpt ON rx.safety_report_id = rpt.safety_report_id
JOIN gold_drugs d ON rx.drug_id = d.drug_id;


-- ---------- Sanity checks ----------

-- gold_drugs: expect 4 rows (XYREM, XYWAV, WAKIX, SUNOSI)
SELECT 'gold_drugs' AS table_name, COUNT(*) AS row_count FROM gold_drugs;

-- gold_reports: expect ~1,968 rows (unique_reports count from Phase 2)
SELECT 'gold_reports' AS table_name, COUNT(*) AS row_count FROM gold_reports;

-- gold_reactions: expect 26,382 rows (matches warehouse total)
SELECT 'gold_reactions' AS table_name, COUNT(*) AS row_count FROM gold_reactions;

-- The view: should equal gold_reactions count (one row per reaction)
SELECT 'gold_analysis_view' AS table_name, COUNT(*) AS row_count FROM gold_analysis_view;

-- Peek at each gold table
SELECT * FROM gold_drugs ORDER BY drug_id;

SELECT * FROM gold_reports ORDER BY receive_date DESC LIMIT 5;

-- Most common reactions for Xyrem, via the view
SELECT drug_name, reaction_term, COUNT(*) AS n
FROM gold_analysis_view
WHERE drug_name = 'XYREM'
GROUP BY drug_name, reaction_term
ORDER BY n DESC
LIMIT 10;