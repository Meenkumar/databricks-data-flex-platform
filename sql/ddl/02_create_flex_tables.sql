-- Create member dimension table with SCD Type 2

CREATE TABLE IF NOT EXISTS main.silver.dim_member (
    member_key BIGINT GENERATED ALWAYS AS IDENTITY,
    member_id STRING NOT NULL,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    ssn STRING,
    phone_number STRING,
    email STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    state STRING,
    zip_code STRING,

    -- SCD Type 2 columns
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL,

    -- Audit columns
    created_date TIMESTAMP NOT NULL,
    updated_date TIMESTAMP NOT NULL,
    pipeline_load_timestamp TIMESTAMP,

    -- Hash for change detection
    _hash STRING
)
USING DELTA
PARTITIONED BY (is_current)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.targetFileSize' = '128mb'
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_member_id ON main.silver.dim_member(member_id);
CREATE INDEX IF NOT EXISTS idx_member_current ON main.silver.dim_member(member_id, is_current);

-- Add table comments
COMMENT ON TABLE main.silver.dim_member IS 'Member dimension with SCD Type 2 tracking';
COMMENT ON COLUMN main.silver.dim_member.member_key IS 'Surrogate key';
COMMENT ON COLUMN main.silver.dim_member.member_id IS 'Business key';
COMMENT ON COLUMN main.silver.dim_member.effective_date IS 'Start date of this version';
COMMENT ON COLUMN main.silver.dim_member.end_date IS 'End date of this version (NULL if current)';
COMMENT ON COLUMN main.silver.dim_member.is_current IS 'Flag indicating if this is the current version';
