# Tracking ID System

## Overview
This document describes the physical table-based tracking ID generation system used for the `card_benefit_load_ci` table, replacing the SQL Server sequence approach.

## Architecture

### Physical Table: `flex.tracking_id_generator`
A dedicated table stores and manages tracking ID counters for various processes.

**Schema:**
```sql
CREATE TABLE flex.tracking_id_generator (
    GeneratorID INT NOT NULL PRIMARY KEY,
    GeneratorName STRING NOT NULL,
    NextTrackingID BIGINT NOT NULL,
    LastUpdated TIMESTAMP NOT NULL,
    UpdatedBy STRING
);
```

**Fields:**
- `GeneratorID`: Unique identifier for each generator (Primary Key)
- `GeneratorName`: Descriptive name of the process using this generator
- `NextTrackingID`: The next available tracking ID to be allocated
- `LastUpdated`: Timestamp of last update
- `UpdatedBy`: Process or user that last updated the counter

## Setup

### 1. Create the Tracking ID Table
Run the SQL script to create and initialize the table:
```bash
databricks sql execute -f sql/create_tracking_id_table.sql
```

Or manually execute:
```sql
CREATE TABLE IF NOT EXISTS flex.tracking_id_generator (
    GeneratorID INT NOT NULL,
    GeneratorName STRING NOT NULL,
    NextTrackingID BIGINT NOT NULL,
    LastUpdated TIMESTAMP NOT NULL,
    UpdatedBy STRING,
    CONSTRAINT pk_tracking_id_generator PRIMARY KEY (GeneratorID)
);

INSERT INTO flex.tracking_id_generator (GeneratorID, GeneratorName, NextTrackingID, LastUpdated, UpdatedBy)
VALUES (1, 'card_benefit_load_ci', 1, CURRENT_TIMESTAMP(), 'SYSTEM')
ON CONFLICT (GeneratorID) DO NOTHING;
```

### 2. Verify Setup
```sql
SELECT * FROM flex.tracking_id_generator WHERE GeneratorName = 'card_benefit_load_ci';
```

## How It Works

### Process Flow
1. **Count Records**: When inserting records, the system first counts how many records need TrackingIDs
2. **Allocate IDs**: Call `get_next_tracking_ids()` which:
   - Reads the current `NextTrackingID` from the table
   - Calculates the range needed (current_id to current_id + count - 1)
   - Updates the table with the new `NextTrackingID` value
   - Returns the starting ID for this batch
3. **Assign IDs**: Add TrackingID column to DataFrame using row_number() + starting ID
4. **Insert Data**: Write records with assigned TrackingIDs to the target table

### Example
```python
# If NextTrackingID = 1000 and we have 50 records to insert:
# 1. get_next_tracking_ids() returns 1000
# 2. Assigns IDs 1000-1049 to the 50 records
# 3. Updates NextTrackingID to 1050
# 4. Next batch will start at 1050
```

## Code Reference

### Function: `get_next_tracking_ids()`
Located in: [src/common/card_issue_functions.py](../src/common/card_issue_functions.py)

**Purpose**: Atomically allocates a batch of tracking IDs

**Parameters:**
- `spark`: SparkSession instance
- `catalog`: Catalog name
- `count`: Number of tracking IDs needed

**Returns:** Starting tracking ID for the batch

**Usage:**
```python
start_id = get_next_tracking_ids(spark, catalog, record_count)
```

### Function: `insert_card_benefit_load_ci()`
Located in: [src/common/card_issue_functions.py](../src/common/card_issue_functions.py)

**Modified to:**
- Call `get_next_tracking_ids()` to get starting ID
- Add `TrackingID` column using window function
- Insert records with sequential TrackingIDs

## Benefits Over Sequence

1. **Platform Independence**: Works on any database platform (Databricks, PostgreSQL, etc.)
2. **Visibility**: Easy to query and monitor ID allocation history
3. **Auditability**: Tracks when and by whom IDs were allocated
4. **Flexibility**: Can reset or adjust counters as needed
5. **Multiple Generators**: Single table can manage IDs for multiple processes

## Monitoring

### Check Current Counter
```sql
SELECT 
    GeneratorName,
    NextTrackingID,
    LastUpdated,
    UpdatedBy
FROM flex.tracking_id_generator
WHERE GeneratorName = 'card_benefit_load_ci';
```

### View Allocation History (requires audit trail)
If you add an audit table, you can track all allocations:
```sql
-- Example audit table (optional)
CREATE TABLE flex.tracking_id_audit (
    AuditID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    GeneratorName STRING,
    StartID BIGINT,
    EndID BIGINT,
    RecordCount INT,
    AllocatedAt TIMESTAMP,
    AllocatedBy STRING
);
```

## Troubleshooting

### Issue: "Tracking ID generator not initialized"
**Solution**: Run the initialization SQL script:
```bash
databricks sql execute -f sql/create_tracking_id_table.sql
```

### Issue: Duplicate TrackingIDs
**Cause**: Concurrent processes updating the same generator
**Solution**: Ensure atomic updates or implement row-level locking:
```sql
-- Use optimistic locking
UPDATE flex.tracking_id_generator
SET NextTrackingID = NextTrackingID + {count},
    LastUpdated = CURRENT_TIMESTAMP()
WHERE GeneratorID = 1 
  AND NextTrackingID = {expected_current_value};
```

### Issue: Need to reset counter
```sql
UPDATE flex.tracking_id_generator
SET NextTrackingID = 1,
    LastUpdated = CURRENT_TIMESTAMP(),
    UpdatedBy = 'MANUAL_RESET'
WHERE GeneratorName = 'card_benefit_load_ci';
```

## Future Enhancements

1. **Add audit trail table** to track all ID allocations
2. **Implement optimistic locking** for concurrent process safety
3. **Add monitoring alerts** when counter approaches max value
4. **Create additional generators** for other processes that need unique IDs
