-- View for active eligibility records
CREATE OR REPLACE VIEW main.gold.vw_active_eligibility AS
SELECT
    e.eligibility_id,
    e.member_id,
    m.first_name,
    m.last_name,
    e.plan_id,
    e.coverage_type,
    e.effective_date,
    e.termination_date,
    e.status,
    e.premium_amount,
    e.deductible_amount,
    DATEDIFF(DAY, e.effective_date, COALESCE(e.termination_date, CURRENT_DATE())) as coverage_days
FROM main.silver.fact_eligibility e
INNER JOIN main.gold.vw_current_members m
    ON e.member_id = m.member_id
WHERE e.status = 'ACTIVE'
    AND e.effective_date <= CURRENT_DATE()
    AND (e.termination_date IS NULL OR e.termination_date >= CURRENT_DATE());

COMMENT ON VIEW main.gold.vw_active_eligibility IS 'Currently active member eligibility records';
