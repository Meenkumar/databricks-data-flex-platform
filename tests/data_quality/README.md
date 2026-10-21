# Data Quality Tests

This directory contains data quality check notebooks and scripts.

## Organization

- **table_level/**: Tests that validate entire tables (row counts, schema compliance)
- **field_level/**: Tests for specific field validations
- **business_rules/**: Tests for complex business rule validations
- **reconciliation/**: Source-to-target reconciliation tests

## Running Tests

### From Databricks Notebook
```python
%run ./eligibility_checks
```

### From Command Line
```bash
databricks jobs run-now --job-id <job-id>
```

## Test Framework

We use Great Expectations for data quality validation:
- Expectations are defined in YAML
- Results are stored in the audit schema
- Failures trigger alerts

## Adding New Tests

1. Create a new notebook in the appropriate subdirectory
2. Define expectations using Great Expectations syntax
3. Add the notebook to the data quality job workflow
4. Configure alerting thresholds
