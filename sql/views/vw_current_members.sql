-- View for current member records only
CREATE OR REPLACE VIEW main.gold.vw_current_members AS
SELECT
    member_key,
    member_id,
    first_name,
    last_name,
    date_of_birth,
    gender,
    phone_number,
    email,
    CONCAT_WS(', ', address_line1, address_line2, city, state, zip_code) as full_address,
    city,
    state,
    zip_code,
    effective_date,
    created_date,
    updated_date
FROM main.silver.dim_member
WHERE is_current = TRUE;

COMMENT ON VIEW main.gold.vw_current_members IS 'Current active member records (SCD Type 2 current versions only)';
