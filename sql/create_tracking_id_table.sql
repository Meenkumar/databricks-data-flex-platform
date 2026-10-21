-- Create tracking ID table to replace sequence
-- This table stores the next available TrackingID for card benefit load operations

CREATE TABLE IF NOT EXISTS flex.tracking_id_generator (
    GeneratorID INT NOT NULL,
    GeneratorName STRING NOT NULL,
    NextTrackingID BIGINT NOT NULL,
    LastUpdated TIMESTAMP NOT NULL,
    UpdatedBy STRING,
    CONSTRAINT pk_tracking_id_generator PRIMARY KEY (GeneratorID)
);

-- Initialize with starting value
INSERT INTO flex.tracking_id_generator (GeneratorID, GeneratorName, NextTrackingID, LastUpdated, UpdatedBy)
VALUES (1, 'card_benefit_load_ci', 1, CURRENT_TIMESTAMP(), 'SYSTEM')
ON CONFLICT (GeneratorID) DO NOTHING;
