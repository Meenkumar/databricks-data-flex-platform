# Quick Start Guide

Get up and running with the Databricks Data Platform in under 30 minutes!

## Prerequisites Checklist

- [ ] Python 3.9 or higher installed
- [ ] Access to a Databricks workspace
- [ ] Databricks CLI installed (`pip install databricks-cli`)
- [ ] Git installed
- [ ] Access to source systems (SQL Server, SFTP)

## Step 1: Clone and Setup (5 minutes)

```bash
# Clone the repository
git clone <repository-url>
cd databricks-data-platform

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.venv\Scripts\activate
# On Mac/Linux:
source .venv/bin/activate

# Install dependencies
pip install -r requirements-dev.txt

# Install package in development mode
pip install -e .
```

## Step 2: Configure Databricks CLI (5 minutes)

```bash
# Configure Databricks CLI
databricks configure --token

# You'll be prompted for:
# - Databricks Host: https://your-workspace.cloud.databricks.com
# - Token: (generate from User Settings > Access Tokens)

# Verify connection
databricks workspace ls /
```

## Step 3: Set Up Secrets (5 minutes)

```bash
# Create secret scope
databricks secrets create-scope --scope data-platform

# Add database credentials
databricks secrets put --scope data-platform --key sql-server-username
databricks secrets put --scope data-platform --key sql-server-password

# Add SFTP credentials
databricks secrets put --scope data-platform --key sftp-username
databricks secrets put --scope data-platform --key sftp-password

# Verify secrets
databricks secrets list --scope data-platform
```

## Step 4: Update Configuration (5 minutes)

Edit `config/member_pipeline_dev.yml`:

```yaml
database:
  jdbc_url: jdbc:sqlserver://YOUR_SERVER:1433;database=YOUR_DB
  username: ${sql-server-username}  # References secret
  password: ${sql-server-password}
```

Edit `config/eligibility_pipeline_dev.yml`:

```yaml
sftp:
  host: YOUR_SFTP_HOST
  username: ${sftp-username}
  password: ${sftp-password}
```

## Step 5: Create Database Schemas (3 minutes)

```bash
# Upload DDL scripts
databricks workspace import sql/ddl/01_create_schemas.sql \
  /Workspace/setup/01_create_schemas.sql

# Run in Databricks SQL Editor or notebook:
# %sql
# CREATE SCHEMA IF NOT EXISTS main.bronze;
# CREATE SCHEMA IF NOT EXISTS main.silver;
# CREATE SCHEMA IF NOT EXISTS main.gold;
# CREATE SCHEMA IF NOT EXISTS main.audit;
```

Or run this notebook command:

```python
# In a Databricks notebook
spark.sql("CREATE SCHEMA IF NOT EXISTS main.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.gold")
spark.sql("CREATE SCHEMA IF NOT EXISTS main.audit")
```

## Step 6: Deploy Code (5 minutes)

```bash
# Validate configuration
python scripts/validate_config.py --environment dev

# Deploy to Databricks
./scripts/deploy.sh dev --skip-tests
```

Or deploy manually:

```bash
# Upload source code
databricks workspace import-dir src /Workspace/src --overwrite

# Upload notebooks
databricks workspace import-dir notebooks /Workspace/notebooks --overwrite

# Upload SQL scripts
databricks workspace import-dir sql /Workspace/sql --overwrite
```

## Step 7: Create Jobs (3 minutes)

```bash
# Create member load job
databricks jobs create --json-file resources/jobs/member_load.yml

# Create eligibility load job
databricks jobs create --json-file resources/jobs/eligibility_load.yml

# List jobs to verify
databricks jobs list
```

## Step 8: Test Run (5 minutes)

### Option A: Run via Jobs UI

1. Go to Databricks workspace > Workflows > Jobs
2. Find "Member Load Job"
3. Click "Run Now"
4. Monitor execution

### Option B: Run via CLI

```bash
# Get job ID
JOB_ID=$(databricks jobs list --output JSON | jq '.jobs[] | select(.settings.name=="member_load_job") | .job_id')

# Run job
databricks jobs run-now --job-id $JOB_ID

# Check run status
databricks runs list --job-id $JOB_ID
```

### Option C: Run Directly in Notebook

1. Open `/Workspace/notebooks/orchestration/main.py`
2. Attach to a cluster
3. Set widget parameters:
   - `pipeline_name`: member
   - `environment`: dev
4. Run all cells

## Verification Checklist

After running, verify:

- [ ] Check audit logs:
  ```sql
  SELECT * FROM main.audit.pipeline_audit 
  ORDER BY start_time DESC LIMIT 10;
  ```

- [ ] Verify member data:
  ```sql
  SELECT COUNT(*) FROM main.silver.dim_member;
  SELECT * FROM main.silver.dim_member LIMIT 10;
  ```

- [ ] Check data quality:
  ```sql
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT member_id) as unique_members,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records
  FROM main.silver.dim_member;
  ```

## Common Issues & Solutions

### Issue: "Module not found" error

**Solution:**
```bash
# Ensure src/ is in PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:/Workspace/src"

# Or in notebook:
import sys
sys.path.append("/Workspace/src")
```

### Issue: JDBC connection timeout

**Solution:**
- Verify SQL Server is accessible from Databricks
- Check firewall rules
- Verify credentials in secrets
- Test connection:
  ```python
  df = spark.read \
      .format("jdbc") \
      .option("url", "jdbc:sqlserver://...") \
      .option("dbtable", "(SELECT 1 as test) as t") \
      .option("user", username) \
      .option("password", password) \
      .load()
  df.show()
  ```

### Issue: Permission denied on table

**Solution:**
```sql
-- Grant permissions
GRANT SELECT, MODIFY ON SCHEMA main.silver TO `your-user@company.com`;
GRANT SELECT, MODIFY ON SCHEMA main.audit TO `your-user@company.com`;
```

### Issue: File not found on SFTP

**Solution:**
- Verify SFTP credentials
- Check file path and pattern
- Test SFTP connection manually:
  ```bash
  sftp username@sftp-host
  ls /path/to/files
  ```

## Next Steps

1. **Review the data**: Check the loaded data in Unity Catalog
2. **Schedule jobs**: Set up cron schedules for automated runs
3. **Add monitoring**: Set up email alerts for job failures
4. **Customize pipelines**: Adapt the pipelines to your specific needs
5. **Add more data sources**: Extend with additional pipelines

## Getting Help

- **Documentation**: See `docs/` folder
- **Architecture**: Read `docs/architecture.md`
- **Operations**: See `docs/operations_runbook.md`
- **Issues**: Create a GitHub issue
- **Questions**: Email data-engineering@company.com

## What's Next?

- [ ] Add more pipelines (claims, providers, etc.)
- [ ] Set up CI/CD pipeline
- [ ] Configure monitoring dashboards
- [ ] Implement data quality metrics
- [ ] Add real-time streaming
- [ ] Set up disaster recovery

Congratulations! Your Databricks Data Platform is now running! 🎉
