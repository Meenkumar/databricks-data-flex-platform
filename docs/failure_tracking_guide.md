# Card Issue Failure Tracking Guide

## Overview

The Card Issue Failure Tracking system provides a unified reconciliation table that captures all member failures across the card issuance pipeline. This enables comprehensive dashboard visibility into why members fail at different stages.

## Failure Stages Tracked

### 1. **PLAID_VERIFICATION**
Members who failed Plaid identity verification checks.

**Failure Reasons:**
- `Plaid API Request Failed` - HTTP response status != 200
- `No Plaid Response Found` - Request exists but no matching response
- `Plaid Screening Not Cleared` - Screening status is not "cleared"

### 2. **CARD_PACKAGE**
Members who are eligible but missing card package configuration.

**Failure Reasons:**
- `No Card Package Found` - No PackageID exists for the member
- `Card Package Inactive` - Package exists but IsActive = 0
- `Card Package Validation Failed` - Other package validation issues

### 3. **BUSINESS_RULES**
Members who failed business rule validations.

**Failure Reasons:**
- Failed duplicate checks (Prank filtering)
- Member exception list matches
- CardIssueCondition validation failures
- Missing NHMemberID

### 4. **LOAD_THRESHOLD**
Members excluded due to load threshold limits.

**Failure Reasons:**
- `Excluded by Load Threshold Limit` - Member was eligible but excluded because batch reached configured limit

### 5. **MEMBER_ELIGIBILITY** (Optional)
Members who failed basic eligibility criteria.

**Failure Reasons:**
- Date range validations (RecordEffDate, BenefitStartDate/EndDate)
- Already has active card mapping
- Already processed in CardBenefitLoad_CI
- Missing mailing address

## Table Schema

```sql
CREATE TABLE card_issue_failure_tracking (
    FailureTrackingID BIGINT GENERATED ALWAYS AS IDENTITY,
    NHLinkID STRING,
    NHMemberID STRING,
    InsuranceCarrierID INT,
    InsuranceHealthPlanID INT,
    ClientCode STRING,
    BenefitYear INT,
    Affinity STRING,
    FailureStage STRING,
    FailureReason STRING,
    FailureDetails STRING,
    ProcessingDate DATE,
    CreateDate TIMESTAMP,
    CreateUser STRING,
    IsActive INT DEFAULT 1,
    RunID STRING
)
```

## Pipeline Behavior

### On Each Run

1. **Clear Previous Failures**: At the start of each pipeline run, previous failures for the same `ClientCode`, `BenefitYear`, `Affinity`, and `ProcessingDate` are soft-deleted (IsActive set to 0).

2. **Track New Failures**: As the pipeline progresses, failures at each stage are inserted into the table with the current RunID.

3. **Summary Report**: At the end of the run, a failure summary is displayed showing failure counts by stage and reason.

### Run Identification

Each pipeline execution gets a unique `RunID` (format: `run_<12-char-hex>`) to distinguish between multiple runs on the same day.

## Dashboard Integration

### Key Metrics to Display

1. **Total Failed Members** - Distinct count of NHLinkID
2. **Failures by Stage** - Pie chart or bar chart
3. **Top Failure Reasons** - Table showing FailureReason with counts
4. **Failure Trends** - Line chart over time
5. **Member Detail List** - Searchable table with drill-through

### Sample Dashboard Queries

See [sql/dashboard_failure_queries.sql](../sql/dashboard_failure_queries.sql) for ready-to-use SQL queries.

### Recommended Visualizations

1. **KPI Cards**
   - Total failures today
   - Plaid failures
   - Package failures
   - Business rule failures

2. **Stage Breakdown** (Pie/Donut Chart)
   ```sql
   SELECT FailureStage, COUNT(*) as FailureCount
   FROM card_issue_failure_tracking
   WHERE ProcessingDate = CURRENT_DATE() AND IsActive = 1
   GROUP BY FailureStage
   ```

3. **Failure Details** (Data Table)
   - Sortable/filterable by all columns
   - Export capability for investigation
   - Drill-through to member details

4. **Trend Analysis** (Line Chart)
   - X-axis: ProcessingDate (last 30 days)
   - Y-axis: Failure count
   - Multiple lines for each FailureStage

## Re-Run Behavior

### What Happens on Re-Run?

When you re-run the pipeline with the same parameters:

1. **Previous failures are soft-deleted** (IsActive = 0) for that processing date
2. **New failures are inserted** with a new RunID
3. **Dashboard shows only active failures** (IsActive = 1)

### Why This Approach?

- Maintains historical audit trail (old runs preserved with IsActive = 0)
- Dashboard always shows current state
- Can compare runs using RunID
- No duplicate data in active view

### To Keep Historical Runs

If you want to see failures from previous runs:

```sql
SELECT * FROM card_issue_failure_tracking
WHERE IsActive = 0  -- Historical/inactive records
ORDER BY ProcessingDate DESC, CreateDate DESC
```

## Troubleshooting

### Issue: No failures showing in dashboard

**Check:**
1. Verify `IsActive = 1` in your query
2. Check `ProcessingDate` matches your expected date
3. Confirm `ClientCode`, `BenefitYear`, `Affinity` parameters

### Issue: Duplicate failures

**Cause:** Pipeline ran multiple times without clearing old data

**Fix:** The pipeline now automatically clears old runs at startup

### Issue: Missing failure details

**Check:**
1. Ensure all failure tracking functions are called in pipeline
2. Verify table exists: `SHOW TABLES LIKE 'card_issue_failure_tracking'`
3. Check pipeline logs for error messages

## API Usage

### Create Table

```python
from common.failure_tracking import create_failure_tracking_table

create_failure_tracking_table(
    spark=spark,
    catalog="data_nonpii_dev",
    database="flex"
)
```

### Clear Previous Runs

```python
from common.failure_tracking import clear_previous_run_failures

clear_previous_run_failures(
    spark=spark,
    catalog="data_nonpii_dev",
    database="flex",
    client_code="H225",
    benefit_year=2026,
    affinity="Igloo"
)
```

### Track Failures

See [src/common/failure_tracking.py](../src/common/failure_tracking.py) for all available functions:
- `track_plaid_failures()`
- `track_card_package_failures()`
- `track_member_validation_failures()`

### Get Summary

```python
from common.failure_tracking import get_failure_summary

summary_df = get_failure_summary(
    spark=spark,
    catalog="data_nonpii_dev",
    database="flex",
    client_code="H225",
    benefit_year=2026,
    affinity="Igloo"
)
summary_df.show()
```

## Data Retention

### Recommendation

Keep failure tracking data for:
- **Active records** (IsActive = 1): Current run only
- **Historical records** (IsActive = 0): 90 days

### Cleanup Query

```sql
DELETE FROM card_issue_failure_tracking
WHERE IsActive = 0
AND ProcessingDate < DATE_SUB(CURRENT_DATE(), 90)
```

## Best Practices

1. **Always filter by IsActive = 1** for current failures
2. **Use RunID** to compare specific runs
3. **Include ProcessingDate** for trend analysis
4. **Cache member details** for drill-through investigations
5. **Export failure lists** for operations teams to investigate
6. **Set up alerts** for sudden spikes in failures
7. **Review FailureDetails** for debugging specific issues

## Support

For questions or issues with failure tracking:
1. Check pipeline logs for error messages
2. Review [failure_tracking.py](../src/common/failure_tracking.py) source code
3. Contact the data engineering team
