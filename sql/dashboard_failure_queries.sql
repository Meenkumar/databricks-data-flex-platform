-- Dashboard Queries for Card Issue Failure Tracking
-- Use these queries to build your dashboard visualizations

-- =============================================================================
-- 1. FAILURE SUMMARY BY STAGE (for pie chart or bar chart)
-- =============================================================================
SELECT
    FailureStage,
    COUNT(*) as FailureCount,
    COUNT(DISTINCT NHLinkID) as UniqueMembers,
    COUNT(DISTINCT CONCAT(NHLinkID, '-', InsuranceCarrierID)) as UniqueMemberCarriers
FROM ${catalog}.${database}.card_issue_failure_tracking
WHERE ClientCode = '${client_code}'
AND BenefitYear = ${benefit_year}
AND Affinity = '${affinity}'
AND ProcessingDate = CURRENT_DATE()
AND IsActive = 1
GROUP BY FailureStage
ORDER BY FailureCount DESC;

-- =============================================================================
-- 2. FAILURE BREAKDOWN BY REASON (detailed drill-down)
-- =============================================================================
SELECT
    FailureStage,
    FailureReason,
    COUNT(*) as FailureCount,
    COUNT(DISTINCT NHLinkID) as UniqueMembers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as PercentageOfTotal
FROM ${catalog}.${database}.card_issue_failure_tracking
WHERE ClientCode = '${client_code}'
AND BenefitYear = ${benefit_year}
AND Affinity = '${affinity}'
AND ProcessingDate = CURRENT_DATE()
AND IsActive = 1
GROUP BY FailureStage, FailureReason
ORDER BY FailureStage, FailureCount DESC;

-- =============================================================================
-- 3. FAILED MEMBERS DETAIL LIST (for table view with drill-through)
-- =============================================================================
SELECT
    NHLinkID,
    NHMemberID,
    InsuranceCarrierID,
    InsuranceHealthPlanID,
    FailureStage,
    FailureReason,
    FailureDetails,
    ProcessingDate,
    CreateDate,
    RunID
FROM ${catalog}.${database}.card_issue_failure_tracking
WHERE ClientCode = '${client_code}'
AND BenefitYear = ${benefit_year}
AND Affinity = '${affinity}'
AND ProcessingDate = CURRENT_DATE()
AND IsActive = 1
ORDER BY FailureStage, CreateDate DESC;

-- =============================================================================
-- 4. PLAID VERIFICATION FAILURES DETAIL
-- =============================================================================
SELECT
    NHLinkID,
    NHMemberID,
    InsuranceCarrierID,
    FailureReason,
    FailureDetails,
    ProcessingDate
FROM ${catalog}.${database}.card_issue_failure_tracking
WHERE FailureStage = 'PLAID_VERIFICATION'
AND ClientCode = '${client_code}'
AND BenefitYear = ${benefit_year}
AND Affinity = '${affinity}'
AND ProcessingDate = CURRENT_DATE()
AND IsActive = 1
ORDER BY CreateDate DESC;

-- =============================================================================
-- 5. CARD PACKAGE FAILURES DETAIL
-- =============================================================================
SELECT
    NHLinkID,
    NHMemberID,
    InsuranceCarrierID,
    InsuranceHealthPlanID,
    FailureReason,
    FailureDetails,
    ProcessingDate
FROM ${catalog}.${database}.card_issue_failure_tracking
WHERE FailureStage = 'CARD_PACKAGE'
AND ClientCode = '${client_code}'
AND BenefitYear = ${benefit_year}
AND Affinity = '${affinity}'
AND ProcessingDate = CURRENT_DATE()
AND IsActive = 1
ORDER BY CreateDate DESC;

-- =============================================================================
-- 6. BUSINESS RULES FAILURES DETAIL
-- =============================================================================
SELECT
    NHLinkID,
    NHMemberID,
    InsuranceCarrierID,
    InsuranceHealthPlanID,
    FailureReason,
    FailureDetails,
    ProcessingDate
FROM ${catalog}.${database}.card_issue_failure_tracking
WHERE FailureStage = 'BUSINESS_RULES'
AND ClientCode = '${client_code}'
AND BenefitYear = ${benefit_year}
AND Affinity = '${affinity}'
AND ProcessingDate = CURRENT_DATE()
AND IsActive = 1
ORDER BY CreateDate DESC;

-- =============================================================================
-- 7. FAILURE TRENDS OVER TIME (for line chart)
-- =============================================================================
SELECT
    ProcessingDate,
    FailureStage,
    COUNT(*) as FailureCount,
    COUNT(DISTINCT NHLinkID) as UniqueMembers
FROM ${catalog}.${database}.card_issue_failure_tracking
WHERE ClientCode = '${client_code}'
AND BenefitYear = ${benefit_year}
AND Affinity = '${affinity}'
AND ProcessingDate >= DATE_SUB(CURRENT_DATE(), 30)  -- Last 30 days
AND IsActive = 1
GROUP BY ProcessingDate, FailureStage
ORDER BY ProcessingDate DESC, FailureStage;

-- =============================================================================
-- 8. DAILY SUMMARY FOR DASHBOARD KPIs
-- =============================================================================
SELECT
    COUNT(*) as TotalFailures,
    COUNT(DISTINCT NHLinkID) as TotalFailedMembers,
    COUNT(DISTINCT CASE WHEN FailureStage = 'PLAID_VERIFICATION' THEN NHLinkID END) as PlaidFailures,
    COUNT(DISTINCT CASE WHEN FailureStage = 'CARD_PACKAGE' THEN NHLinkID END) as PackageFailures,
    COUNT(DISTINCT CASE WHEN FailureStage = 'BUSINESS_RULES' THEN NHLinkID END) as BusinessRuleFailures,
    COUNT(DISTINCT CASE WHEN FailureStage = 'LOAD_THRESHOLD' THEN NHLinkID END) as ThresholdExclusions,
    MAX(CreateDate) as LastUpdated,
    COUNT(DISTINCT RunID) as TotalRuns
FROM ${catalog}.${database}.card_issue_failure_tracking
WHERE ClientCode = '${client_code}'
AND BenefitYear = ${benefit_year}
AND Affinity = '${affinity}'
AND ProcessingDate = CURRENT_DATE()
AND IsActive = 1;

-- =============================================================================
-- 9. MEMBER-LEVEL COMPLETE FAILURE HISTORY (for member drill-down)
-- =============================================================================
SELECT
    NHLinkID,
    NHMemberID,
    InsuranceCarrierID,
    InsuranceHealthPlanID,
    STRING_AGG(DISTINCT FailureStage, ', ') as AllFailureStages,
    STRING_AGG(DISTINCT FailureReason, '; ') as AllFailureReasons,
    COUNT(*) as TotalFailurePoints,
    MAX(CreateDate) as LastFailureDate
FROM ${catalog}.${database}.card_issue_failure_tracking
WHERE ClientCode = '${client_code}'
AND BenefitYear = ${benefit_year}
AND Affinity = '${affinity}'
AND ProcessingDate = CURRENT_DATE()
AND IsActive = 1
GROUP BY NHLinkID, NHMemberID, InsuranceCarrierID, InsuranceHealthPlanID
ORDER BY TotalFailurePoints DESC, LastFailureDate DESC;

-- =============================================================================
-- 10. COMPARE CURRENT RUN VS PREVIOUS RUN (for run-over-run comparison)
-- =============================================================================
WITH LatestTwoRuns AS (
    SELECT DISTINCT
        RunID,
        ProcessingDate,
        ROW_NUMBER() OVER (ORDER BY ProcessingDate DESC, CreateDate DESC) as rn
    FROM ${catalog}.${database}.card_issue_failure_tracking
    WHERE ClientCode = '${client_code}'
    AND BenefitYear = ${benefit_year}
    AND Affinity = '${affinity}'
    AND IsActive = 1
)
SELECT
    f.FailureStage,
    SUM(CASE WHEN r.rn = 1 THEN 1 ELSE 0 END) as CurrentRunFailures,
    SUM(CASE WHEN r.rn = 2 THEN 1 ELSE 0 END) as PreviousRunFailures,
    SUM(CASE WHEN r.rn = 1 THEN 1 ELSE 0 END) - SUM(CASE WHEN r.rn = 2 THEN 1 ELSE 0 END) as Difference
FROM ${catalog}.${database}.card_issue_failure_tracking f
JOIN LatestTwoRuns r ON f.RunID = r.RunID
WHERE f.ClientCode = '${client_code}'
AND f.BenefitYear = ${benefit_year}
AND f.Affinity = '${affinity}'
AND f.IsActive = 1
GROUP BY f.FailureStage
ORDER BY CurrentRunFailures DESC;

-- =============================================================================
-- USAGE NOTES:
-- Replace the following placeholders with actual values:
-- ${catalog} = e.g., data_nonpii_dev
-- ${database} = e.g., flex
-- ${client_code} = e.g., H225
-- ${benefit_year} = e.g., 2026
-- ${affinity} = e.g., Igloo
-- =============================================================================
