-- =====================================================================================================
-- Gold layer in Spark SQL (same logic as src/lakehouse/gold.py).
--
-- Inputs (silver):  silver_claim_header, silver_member_month, silver_member_year, silver_icd9_dx
-- Locally these are temp views registered by src/run_gold_sql.py, which runs this file statement by
-- statement and checks every mart against the PySpark output (results/sql_parity.csv).
-- On Databricks / Unity Catalog swap "CREATE OR REPLACE TEMP VIEW gold_x" for
-- "CREATE OR REPLACE TABLE main.claims_gold.x" and the silver_* names for main.claims_silver.*.
-- =====================================================================================================

-- 1) Cost per member per month (PMPM) by service month and claim type --------------------------------
CREATE OR REPLACE TEMP VIEW gold_pmpm_monthly AS
WITH member_months AS (
    SELECT month_start AS service_month, COUNT(DISTINCT member_id) AS member_months
    FROM silver_member_month
    GROUP BY month_start
),
paid AS (
    SELECT service_month,
           SUM(CASE WHEN claim_type = 'IP'  THEN paid_amt ELSE 0 END) AS paid_ip,
           SUM(CASE WHEN claim_type = 'OP'  THEN paid_amt ELSE 0 END) AS paid_op,
           SUM(CASE WHEN claim_type = 'CAR' THEN paid_amt ELSE 0 END) AS paid_carrier
    FROM silver_claim_header
    GROUP BY service_month
)
SELECT m.service_month,
       m.member_months,
       COALESCE(p.paid_ip, 0)      AS paid_ip,
       COALESCE(p.paid_op, 0)      AS paid_op,
       COALESCE(p.paid_carrier, 0) AS paid_carrier,
       COALESCE(p.paid_ip, 0) + COALESCE(p.paid_op, 0) + COALESCE(p.paid_carrier, 0) AS paid_total,
       ROUND((COALESCE(p.paid_ip, 0) + COALESCE(p.paid_op, 0) + COALESCE(p.paid_carrier, 0))
             / m.member_months, 2) AS pmpm_total
FROM member_months m
LEFT JOIN paid p USING (service_month);

-- 2) PMPM by chronic-condition risk segment ------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW gold_pmpm_by_risk_segment AS
WITH seg AS (
    SELECT member_id, coverage_year, member_months,
           CASE WHEN chronic_condition_count = 0 THEN '0 chronic conditions'
                WHEN chronic_condition_count <= 2 THEN '1-2 chronic conditions'
                WHEN chronic_condition_count <= 4 THEN '3-4 chronic conditions'
                ELSE '5+ chronic conditions' END AS risk_segment
    FROM silver_member_year
    WHERE member_months > 0
),
paid AS (
    SELECT member_id, service_year AS coverage_year, SUM(paid_amt) AS paid
    FROM silver_claim_header
    GROUP BY member_id, service_year
)
SELECT s.coverage_year, s.risk_segment,
       COUNT(DISTINCT s.member_id)  AS members,
       SUM(s.member_months)         AS member_months,
       SUM(COALESCE(p.paid, 0))     AS paid_total,
       ROUND(SUM(COALESCE(p.paid, 0)) / SUM(s.member_months), 2) AS pmpm
FROM seg s
LEFT JOIN paid p USING (member_id, coverage_year)
GROUP BY s.coverage_year, s.risk_segment;

-- 3) 30-day all-cause readmissions ---------------------------------------------------------------------
--    A stay counts as readmitted when the member's next admission is 1-30 days after discharge
--    (same-day re-admission = transfer). Index stays discharged in the last 30 days of data are excluded.
CREATE OR REPLACE TEMP VIEW gold_readmission_index_stays AS
WITH ordered AS (
    SELECT member_id, claim_id, admit_dt, discharge_dt, provider_id, principal_dx, principal_dx_chapter,
           drg_cd, paid_amt, length_of_stay,
           LEAD(admit_dt) OVER (PARTITION BY member_id ORDER BY admit_dt, discharge_dt, claim_id) AS next_admit_dt
    FROM silver_claim_header
    WHERE claim_type = 'IP'
)
SELECT *,
       DATEDIFF(next_admit_dt, discharge_dt)                                   AS days_to_next_admit,
       COALESCE(DATEDIFF(next_admit_dt, discharge_dt) BETWEEN 1 AND 30, FALSE) AS readmit_30d,
       DATEDIFF(DATE'2010-12-31', discharge_dt) >= 30                          AS has_full_followup,
       YEAR(discharge_dt)                                                      AS discharge_year
FROM ordered;

CREATE OR REPLACE TEMP VIEW gold_readmission_summary AS
SELECT 'overall' AS dimension, 'all' AS value, COUNT(*) AS index_stays,
       SUM(CAST(readmit_30d AS INT)) AS readmissions_30d,
       ROUND(SUM(CAST(readmit_30d AS INT)) / COUNT(*), 4) AS readmit_rate_30d
FROM gold_readmission_index_stays WHERE has_full_followup
UNION ALL
SELECT 'discharge_year', CAST(discharge_year AS STRING), COUNT(*), SUM(CAST(readmit_30d AS INT)),
       ROUND(SUM(CAST(readmit_30d AS INT)) / COUNT(*), 4)
FROM gold_readmission_index_stays WHERE has_full_followup GROUP BY discharge_year
UNION ALL
SELECT 'principal_dx_chapter', principal_dx_chapter, COUNT(*), SUM(CAST(readmit_30d AS INT)),
       ROUND(SUM(CAST(readmit_30d AS INT)) / COUNT(*), 4)
FROM gold_readmission_index_stays WHERE has_full_followup GROUP BY principal_dx_chapter;

-- 4) Top principal diagnoses by total paid -------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW gold_top_diagnoses_by_cost AS
WITH total AS (SELECT SUM(paid_amt) AS paid FROM silver_claim_header),
by_dx AS (
    SELECT principal_dx,
           COUNT(*)                  AS claims,
           COUNT(DISTINCT member_id) AS members,
           SUM(paid_amt)             AS paid_total
    FROM silver_claim_header
    WHERE principal_dx IS NOT NULL
    GROUP BY principal_dx
),
ranked AS (
    SELECT b.*, ROW_NUMBER() OVER (ORDER BY paid_total DESC, principal_dx) AS cost_rank,
           ROUND(b.paid_total / t.paid, 4) AS share_of_total_paid
    FROM by_dx b CROSS JOIN total t
)
SELECT r.cost_rank, r.principal_dx, d.icd9_display, d.description, d.icd9_chapter,
       r.claims, r.members, r.paid_total, r.share_of_total_paid,
       ROUND(r.paid_total / r.claims, 2) AS paid_per_claim
FROM ranked r
LEFT JOIN silver_icd9_dx d ON d.icd9_code = r.principal_dx
WHERE r.cost_rank <= 50;

-- 5) Provider utilization ------------------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW gold_provider_utilization AS
WITH util AS (
    SELECT claim_type, provider_id,
           COUNT(*) AS claims, COUNT(DISTINCT member_id) AS members, SUM(paid_amt) AS paid_total,
           ROUND(AVG(length_of_stay), 2) AS avg_length_of_stay
    FROM silver_claim_header
    WHERE provider_id IS NOT NULL AND provider_id <> ''
    GROUP BY claim_type, provider_id
),
readm AS (
    SELECT provider_id, 'IP' AS claim_type, COUNT(*) AS index_stays,
           ROUND(AVG(CAST(readmit_30d AS INT)), 4) AS readmit_rate_30d
    FROM gold_readmission_index_stays WHERE has_full_followup
    GROUP BY provider_id
)
SELECT u.*, r.index_stays, r.readmit_rate_30d,
       ROUND(u.paid_total / u.claims, 2)  AS paid_per_claim,
       ROUND(u.paid_total / u.members, 2) AS paid_per_member,
       ROW_NUMBER() OVER (PARTITION BY u.claim_type ORDER BY u.paid_total DESC, u.provider_id) AS paid_rank_in_type
FROM util u
LEFT JOIN readm r USING (claim_type, provider_id);
