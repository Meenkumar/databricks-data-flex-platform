-- Create Card Issue Failure Tracking Table
-- This table consolidates all failure points in the card issuance pipeline
-- for unified dashboard reconciliation

CREATE TABLE IF NOT EXISTS ${catalog}.${database}.card_issue_failure_tracking (
    FailureTrackingID BIGINT GENERATED ALWAYS AS IDENTITY,
    NHLinkID STRING,
    NHMemberID STRING,
    InsuranceCarrierID INT,
    InsuranceHealthPlanID INT,
    ClientCode STRING,
    BenefitYear INT,
    Affinity STRING,
    FailureStage STRING COMMENT 'PLAID_VERIFICATION, CARD_PACKAGE, MEMBER_ELIGIBILITY, BUSINESS_RULES, LOAD_THRESHOLD, CARD_MAPPING, CARD_LOAD',
    FailureReason STRING COMMENT 'Short description of failure reason',
    FailureDetails STRING COMMENT 'Detailed failure information',
    ProcessingDate DATE,
    CreateDate TIMESTAMP,
    CreateUser STRING,
    IsActive INT DEFAULT 1,
    RunID STRING COMMENT 'Unique identifier for each pipeline run'
)
USING delta
COMMENT 'Tracks all card issuance pipeline failures for dashboard reconciliation';

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_failure_tracking_lookup
ON ${catalog}.${database}.card_issue_failure_tracking (ClientCode, BenefitYear, Affinity, ProcessingDate, IsActive);

CREATE INDEX IF NOT EXISTS idx_failure_tracking_member
ON ${catalog}.${database}.card_issue_failure_tracking (NHLinkID, InsuranceCarrierID);

-- Sample usage:
-- Replace ${catalog} with your catalog name (e.g., data_nonpii_dev)
-- Replace ${database} with your database name (e.g., flex)
