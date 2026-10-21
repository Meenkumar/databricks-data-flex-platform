# Databricks Data Platform

A production-ready data platform built on Databricks for processing healthcare member and eligibility data using medallion architecture (Bronze, Silver, Gold).

## Features

- **Medallion Architecture**: Bronze (raw) → Silver (cleansed) → Gold (aggregated)
- **SCD Type 2**: Track historical changes in member dimensions
- **Data Quality**: Comprehensive validation framework
- **Multi-Source Ingestion**: SQL Server, SFTP, REST APIs
- **Automated Workflows**: Scheduled jobs with monitoring and alerting
- **Unity Catalog**: Fine-grained access control and governance
- **Delta Lake**: ACID transactions, time travel, schema evolution

## Project Structure

```
databricks-data-platform/
├── src/                    # Source code
│   ├── common/             # Shared utilities
│   ├── ingestion/          # Data readers
│   ├── transformations/    # Data transformations
│   └── pipelines/          # Pipeline orchestration
├── notebooks/              # Databricks notebooks
│   ├── orchestration/      # Main orchestration
│   └── development/        # Ad-hoc development
├── sql/                    # SQL scripts
│   ├── ddl/                # Table definitions
│   ├── views/              # View definitions
│   ├── procedures/         # Stored procedures
│   └── migrations/         # Database migrations
├── resources/              # Job and pipeline configs
│   ├── jobs/               # Workflow definitions
│   └── pipelines/          # DLT pipelines
├── config/                 # Configuration files
│   ├── schemas/            # Schema definitions
│   ├── mappings/           # Field mappings
│   └── clients/            # Client-specific configs
├── tests/                  # Test suite
│   ├── unit/               # Unit tests
│   ├── integration/        # Integration tests
│   └── data_quality/       # DQ checks
├── scripts/                # Utility scripts
└── docs/                   # Documentation
```

## Quick Start

### Prerequisites

- Python 3.9+
- Databricks CLI
- Access to Databricks workspace

### Local Development

```bash
# Clone repository
git clone <repository-url>
cd databricks-data-platform

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/unit -v
```

### Deployment

```bash
# Deploy to dev environment
./scripts/deploy.sh dev

# Validate configuration
python scripts/validate_config.py --environment dev

# Deploy to production
./scripts/deploy.sh prod
```

## Pipelines

### Member Pipeline
- **Source**: SQL Server database
- **Target**: `main.silver.dim_member`
- **Features**: SCD Type 2, PII cleansing
- **Schedule**: Daily at 2:00 AM EST

### Eligibility Pipeline
- **Source**: SFTP CSV files
- **Target**: `main.silver.fact_eligibility`
- **Features**: Validation, deduplication
- **Schedule**: Daily at 2:30 AM EST

## Configuration

Configuration files use YAML format and support environment-specific overrides:

```yaml
# config/member_pipeline_dev.yml
source_system: sql_server
source_table: Members
target_table: main.silver.dim_member
database:
  jdbc_url: jdbc:sqlserver://...
  username: ${DB_USER}
  password: ${DB_PASSWORD}
```

## Testing

```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit -v

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test file
pytest tests/unit/test_cleansing.py -v
```

## Data Quality

Built-in validation framework with checks for:
- Null values
- Uniqueness constraints
- Value lists (enums)
- Numeric ranges
- Regex patterns

Example:
```python
from transformations.validation import DataValidator

validator = DataValidator(df)
validator.check_not_null(["member_id", "first_name"])
validator.check_unique(["member_id"])
validator.assert_valid()
```

## Monitoring

- **Audit Logs**: `main.audit.pipeline_audit`
- **Dashboards**: Available in Databricks SQL
- **Alerts**: Email and Slack notifications
- **Metrics**: Success rate, execution time, record counts

## Documentation

- [Architecture](docs/architecture.md) - System architecture and design
- [Deployment](docs/deployment.md) - Deployment procedures
- [Operations Runbook](docs/operations_runbook.md) - Operational procedures

## Contributing

1. Create feature branch from `develop`
2. Make changes and add tests
3. Run tests and linting
4. Submit pull request

## License

Copyright © 2026 Company Name. All rights reserved.

## Support

For issues or questions:
- Data Engineering Team: data-engineering@company.com
- Slack: #data-platform
- On-Call: data-eng-oncall@company.com
