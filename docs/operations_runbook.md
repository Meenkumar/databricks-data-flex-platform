# Operations Runbook

## Daily Operations

### Morning Checks
- [ ] Review overnight job executions
- [ ] Check data quality alerts
- [ ] Verify expected row counts
- [ ] Review audit logs for anomalies

### Health Check Query
```sql
SELECT
    pipeline_name,
    status,
    records_read,
    records_written,
    start_time,
    end_time,
    TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes
FROM main.audit.pipeline_audit
WHERE DATE(start_time) = CURRENT_DATE()
ORDER BY start_time DESC;
```

## Common Issues and Resolutions

### Pipeline Failures

#### Member Pipeline Failed

**Symptoms**: Job shows FAILED status, no new records in dim_member

**Diagnosis**:
```sql
-- Check latest audit entry
SELECT * FROM main.audit.pipeline_audit
WHERE pipeline_name = 'member_pipeline'
ORDER BY start_time DESC
LIMIT 1;

-- Check for specific errors
SELECT error_message FROM main.audit.pipeline_audit
WHERE pipeline_name = 'member_pipeline' AND status = 'FAILED';
```

**Common Causes**:
1. **SQL Server connection timeout**
   - Check network connectivity
   - Verify credentials in secrets
   - Check SQL Server availability

2. **Data quality validation failure**
   - Review validation error details
   - Check source data for issues
   - Adjust validation thresholds if appropriate

3. **Schema mismatch**
   - Compare source schema with expected schema
   - Update schema config if source changed
   - Coordinate with source system team

**Resolution Steps**:
```bash
# 1. Review error logs
databricks jobs get-run --run-id <run-id>

# 2. Fix underlying issue

# 3. Rerun job
databricks jobs run-now --job-id <job-id>
```

#### Eligibility Pipeline Failed

**Common Causes**:
1. **SFTP file not available**
   - Check SFTP server for file
   - Verify file naming pattern
   - Check permissions

2. **Malformed CSV**
   - Validate CSV format
   - Check for encoding issues
   - Review delimiter configuration

**Resolution**:
```bash
# Download file manually to inspect
sftp user@server
get /path/to/file.csv

# Validate CSV format
head -20 file.csv
```

### Performance Issues

#### Slow Pipeline Execution

**Investigation**:
```sql
-- Find slow-running pipelines
SELECT
    pipeline_name,
    AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)) as avg_duration_minutes
FROM main.audit.pipeline_audit
WHERE start_time >= CURRENT_DATE() - INTERVAL 7 DAY
GROUP BY pipeline_name
HAVING avg_duration_minutes > 30;
```

**Optimization Steps**:
1. Check cluster size and auto-scaling settings
2. Review partition strategy
3. Optimize file sizes (target: 128MB)
4. Enable Z-ordering on frequently filtered columns
5. Check for data skew

```sql
-- Optimize table
OPTIMIZE main.silver.dim_member
ZORDER BY (member_id);

-- Vacuum old files (7+ days)
VACUUM main.silver.dim_member RETAIN 168 HOURS;
```

### Data Quality Issues

#### Unexpected Null Values

```sql
-- Check null percentage by column
SELECT
    'member_id' as column_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN member_id IS NULL THEN 1 ELSE 0 END) as null_count,
    (SUM(CASE WHEN member_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*)) as null_percentage
FROM main.silver.dim_member
WHERE is_current = TRUE;
```

#### Duplicate Records

```sql
-- Find duplicates
SELECT member_id, COUNT(*) as count
FROM main.silver.dim_member
WHERE is_current = TRUE
GROUP BY member_id
HAVING COUNT(*) > 1;
```

## Manual Operations

### Reprocessing Data

```python
# Databricks notebook
from pipelines.member_pipeline import MemberPipeline

# Reprocess specific date range
pipeline = MemberPipeline(spark, environment="prod")
# Implement date-range logic in pipeline
pipeline.run()
```

### Backfilling Historical Data

1. Prepare source data
2. Create temporary staging table
3. Run pipeline with backfill flag
4. Validate results
5. Archive staging data

### Refreshing Materialized Views

```sql
REFRESH MATERIALIZED VIEW main.gold.mv_member_summary;
```

## Alerting and Escalation

### Alert Severity Levels

**P1 (Critical)**: Production pipeline down, data unavailable
- Response time: Immediate
- Escalation: Data Engineering Lead + On-call Engineer

**P2 (High)**: Pipeline failing but fallback available
- Response time: Within 1 hour
- Escalation: Data Engineering team

**P3 (Medium)**: Performance degradation
- Response time: Within 4 hours
- Escalation: Data Engineering team

**P4 (Low)**: Warning or informational
- Response time: Next business day
- Escalation: Queue for triage

### Escalation Path

1. On-call Data Engineer
2. Data Engineering Lead
3. Engineering Manager
4. VP of Engineering

## Maintenance Windows

### Weekly Maintenance
- **Day**: Sunday 2:00 AM - 4:00 AM EST
- **Activities**:
  - Table optimization (OPTIMIZE, VACUUM)
  - Index maintenance
  - Log cleanup

### Monthly Maintenance
- **Day**: First Saturday 12:00 AM - 6:00 AM EST
- **Activities**:
  - Major schema migrations
  - Cluster configuration updates
  - Dependency upgrades

## Monitoring Dashboards

### Key Metrics
- Pipeline success rate (target: >99%)
- Average execution time (baseline: 30 min)
- Data quality score (target: >95%)
- Record processing rate
- Error rate by pipeline

### Dashboard Links
- Pipeline Monitoring: `/sql/dashboards/pipeline-monitoring`
- Data Quality: `/sql/dashboards/data-quality`
- Cost Analysis: `/sql/dashboards/cost-analysis`

## On-Call Procedures

### Handoff Checklist
- [ ] Review open incidents
- [ ] Check scheduled maintenance
- [ ] Verify monitoring is active
- [ ] Test alerting (send test alert)
- [ ] Review runbook updates

### Emergency Contacts
- Data Engineering: data-eng-oncall@company.com
- Platform Team: platform-oncall@company.com
- Database Admins: dba-oncall@company.com
