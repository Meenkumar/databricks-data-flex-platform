# Data Platform Architecture

## Overview

This Databricks data platform implements a medallion architecture (Bronze, Silver, Gold) for processing healthcare member and eligibility data.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                              │
├──────────────┬──────────────────┬──────────────────────────────┤
│  SQL Server  │      SFTP        │         REST API             │
│   (Member)   │  (Eligibility)   │     (Future Sources)         │
└──────┬───────┴────────┬─────────┴──────────────┬───────────────┘
       │                │                        │
       v                v                        v
┌─────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER                                 │
│                   (Raw Data - Delta)                             │
│  • Exact copy of source data                                     │
│  • Minimal transformations                                       │
│  • Audit metadata attached                                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           v
┌─────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER                                 │
│              (Cleansed & Validated - Delta)                      │
│  • Data cleansing applied                                        │
│  • Schema enforcement                                            │
│  • Data quality checks                                           │
│  • SCD Type 2 for dimensions                                     │
│  • Deduplication                                                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           v
┌─────────────────────────────────────────────────────────────────┐
│                      GOLD LAYER                                  │
│              (Business-Level - Delta)                            │
│  • Aggregated views                                              │
│  • Business-friendly models                                      │
│  • Analytics-ready datasets                                      │
│  • Materialized views                                            │
└─────────────────────────────────────────────────────────────────┘
```

## Layer Details

### Bronze Layer
- **Purpose**: Store raw data exactly as received
- **Format**: Delta Lake tables
- **Partitioning**: By ingestion date
- **Retention**: 90 days
- **Key Features**:
  - Immutable once written
  - Full audit trail
  - Schema evolution enabled

### Silver Layer
- **Purpose**: Cleansed, validated, enterprise-ready data
- **Format**: Delta Lake tables
- **Key Features**:
  - SCD Type 2 for member dimension
  - Data quality checks enforced
  - PII masking applied where needed
  - Standardized formats

### Gold Layer
- **Purpose**: Business-level aggregations and views
- **Format**: Delta Lake tables and views
- **Key Features**:
  - Optimized for BI tools
  - Pre-aggregated metrics
  - Business-friendly naming

## Data Flow

### Member Pipeline
1. **Ingestion**: Read from SQL Server using JDBC
2. **Validation**: Check required fields, data types, business rules
3. **Cleansing**: Standardize phone, SSN, addresses
4. **SCD Type 2**: Track changes over time
5. **Load**: Write to silver.dim_member

### Eligibility Pipeline
1. **Ingestion**: Download CSV from SFTP
2. **Validation**: Schema validation, date formats
3. **Cleansing**: Trim strings, deduplicate
4. **Enrichment**: Join with member dimension
5. **Load**: Write to silver.fact_eligibility

## Technology Stack

- **Compute**: Databricks (Apache Spark)
- **Storage**: Delta Lake
- **Orchestration**: Databricks Workflows
- **Catalog**: Unity Catalog
- **Languages**: Python, SQL
- **Testing**: pytest
- **CI/CD**: GitHub Actions (future)

## Key Design Decisions

### Why Delta Lake?
- ACID transactions
- Time travel capabilities
- Schema evolution
- Optimized file handling
- Change Data Feed support

### Why SCD Type 2?
- Track historical changes in member data
- Compliance requirements
- Audit trail for investigations

### Why Medallion Architecture?
- Clear separation of concerns
- Incremental quality improvement
- Easier debugging and recovery
- Flexible consumption patterns

## Security

- Unity Catalog for fine-grained access control
- Row-level security on PII columns
- Encryption at rest and in transit
- Audit logging for all data access
- Service principals for automation

## Performance Optimization

- Auto-optimize enabled on all tables
- Z-ordering on frequently filtered columns
- Partition pruning strategies
- File size management (128MB target)
- Caching for hot data

## Monitoring

- Pipeline execution metrics in audit tables
- Data quality check results tracked
- Alerting on failures via email/Slack
- Databricks SQL dashboards for ops visibility
