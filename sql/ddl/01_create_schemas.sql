-- Create database schemas for the data platform
-- Run this first during initial setup

-- Bronze layer (raw data)
CREATE SCHEMA IF NOT EXISTS main.bronze
COMMENT 'Bronze layer - raw ingested data';

-- Silver layer (cleansed and validated)
CREATE SCHEMA IF NOT EXISTS main.silver
COMMENT 'Silver layer - cleansed and validated data';

-- Gold layer (business-level aggregates)
CREATE SCHEMA IF NOT EXISTS main.gold
COMMENT 'Gold layer - business-level aggregates and dimensions';

-- Audit schema
CREATE SCHEMA IF NOT EXISTS main.audit
COMMENT 'Audit and metadata tables';

-- Configuration schema
CREATE SCHEMA IF NOT EXISTS main.config
COMMENT 'Configuration and reference data';
