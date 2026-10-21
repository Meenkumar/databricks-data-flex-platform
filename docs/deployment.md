# Deployment Guide

## Prerequisites

1. **Databricks CLI** installed and configured
   ```bash
   pip install databricks-cli
   databricks configure --token
   ```

2. **Python 3.9+** for local development and testing

3. **Access permissions**:
   - Databricks workspace admin (for initial setup)
   - Unity Catalog CREATE permissions
   - Secrets management access

## Environment Setup

### Development Environment

```bash
# Clone repository
git clone <repository-url>
cd databricks-data-platform

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/unit -v
```

### Databricks Configuration

1. **Create Unity Catalog schemas**:
   ```bash
   databricks workspace import sql/ddl/01_create_schemas.sql /Workspace/setup/schemas.sql
   # Run via Databricks SQL Editor
   ```

2. **Set up secrets**:
   ```bash
   # Create secret scope
   databricks secrets create-scope --scope data-platform

   # Add secrets
   databricks secrets put --scope data-platform --key sql-server-password
   databricks secrets put --scope data-platform --key sftp-private-key
   ```

3. **Configure service principal**:
   - Create service principal in Azure AD
   - Grant necessary permissions in Unity Catalog
   - Store credentials in secrets

## Deployment Process

### Using Deployment Script

```bash
# Deploy to dev
./scripts/deploy.sh dev

# Deploy to QA (with validation only)
./scripts/deploy.sh qa --validate-only

# Deploy to production (requires approval)
./scripts/deploy.sh prod
```

### Manual Deployment

1. **Sync source code**:
   ```bash
   databricks workspace import-dir src /Workspace/src --overwrite
   ```

2. **Sync notebooks**:
   ```bash
   databricks workspace import-dir notebooks /Workspace/notebooks --overwrite
   ```

3. **Create jobs**:
   ```bash
   databricks jobs create --json-file resources/jobs/member_load.yml
   databricks jobs create --json-file resources/jobs/eligibility_load.yml
   ```

4. **Create DLT pipelines**:
   ```bash
   databricks pipelines create --json-file resources/pipelines/dlt_pipeline.yml
   ```

## Configuration Management

### Environment-Specific Configs

Configuration files support environment-specific overrides:

```
config/
  member_pipeline_dev.yml
  member_pipeline_qa.yml
  member_pipeline_prod.yml
```

### Secrets Management

Never commit secrets to version control. Use Databricks secrets:

```python
from common.config_loader import ConfigLoader

# In code, reference secrets:
password = dbutils.secrets.get(scope="data-platform", key="sql-server-password")
```

## Database Migrations

Migrations are versioned SQL scripts in `sql/migrations/`:

```bash
# Create new migration
cat > sql/migrations/V004__add_new_table.sql << EOF
CREATE TABLE IF NOT EXISTS main.silver.new_table (
  id BIGINT,
  name STRING
) USING DELTA;
EOF

# Apply migrations (handled by deploy.sh)
```

## Rollback Procedures

### Code Rollback
```bash
# Revert to previous version
git checkout <previous-commit>
./scripts/deploy.sh <environment>
```

### Data Rollback
```sql
-- Delta time travel
RESTORE TABLE main.silver.dim_member TO VERSION AS OF 10;

-- Or restore to timestamp
RESTORE TABLE main.silver.dim_member TO TIMESTAMP AS OF '2026-10-09 10:00:00';
```

## Smoke Testing

After deployment, run smoke tests:

```bash
# Integration tests
pytest tests/integration -v

# Validate data quality
databricks jobs run-now --job-id <data-quality-job-id>
```

## CI/CD Pipeline

### GitHub Actions Workflow

```yaml
name: Deploy Data Platform

on:
  push:
    branches: [main, develop]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run tests
        run: pytest tests/unit

  deploy:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to Databricks
        run: ./scripts/deploy.sh ${{ github.ref == 'refs/heads/main' && 'prod' || 'dev' }}
```

## Monitoring Post-Deployment

1. Check job execution logs
2. Verify audit table entries
3. Run data quality checks
4. Monitor Databricks SQL dashboards
5. Validate alerting is working

## Troubleshooting

### Common Issues

**Issue**: Job fails with "Module not found"
- **Solution**: Ensure src/ directory is synced and PYTHONPATH is set

**Issue**: Permission denied on table
- **Solution**: Grant appropriate Unity Catalog permissions

**Issue**: JDBC connection timeout
- **Solution**: Check network connectivity and firewall rules

## Environment Promotion

```
Dev → QA → Prod
```

1. **Dev**: Continuous deployment from develop branch
2. **QA**: Manual promotion with validation
3. **Prod**: Requires approval, deployed from main branch
