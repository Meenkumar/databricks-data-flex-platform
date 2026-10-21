"""
Card issuance business logic functions.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, replace, current_timestamp, rank, desc, row_number, monotonically_increasing_id
)
from pyspark.sql.window import Window
from typing import Dict, List
from common.logging import get_logger


logger = get_logger(__name__)


def get_next_tracking_ids(
    spark: SparkSession,
    catalog: str,
    count: int
) -> int:
    """
    Get the next batch of tracking IDs from the physical table and update the counter.

    Args:
        spark: SparkSession instance
        catalog: Catalog name
        count: Number of tracking IDs needed

    Returns:
        Starting tracking ID for the batch
    """
    tracking_table = f"{catalog}.flex.tracking_id_generator"

    # Get current NextTrackingID
    current_id_query = f"""
        SELECT NextTrackingID
        FROM {tracking_table}
        
    """

    result = spark.sql(current_id_query).first()

    if result is None:
        raise ValueError("Tracking ID generator not initialized. Please run create_tracking_id_table.sql")

    start_tracking_id = result["NextTrackingID"]
    next_tracking_id = start_tracking_id + count

    # Update the NextTrackingID
    update_query = f"""
        UPDATE {tracking_table}
        SET NextTrackingID = {next_tracking_id},
            LastUpdated = CURRENT_TIMESTAMP(),
            UpdatedBy = 'card_issue_process'
        WHERE 1=1
    """

    spark.sql(update_query)

    logger.info(f"Allocated tracking IDs from {start_tracking_id} to {next_tracking_id - 1}")

    return start_tracking_id


def get_client_benefit_rules(
    spark: SparkSession,
    catalog: str,
    client_code: str,
    benefit_year: int,
    affinity: str
) -> DataFrame:
    """
    Get active client benefit rules for card issuance.

    Args:
        spark: SparkSession instance
        catalog: Catalog name
        client_code: Client code
        benefit_year: Benefit year
        affinity: Affinity value

    Returns:
        DataFrame with active client benefit rules
    """
    rules_table = f"{catalog}.otcfunds.client_benefit_rules_data"
    carriers_table = f"{catalog}.config.insurance_carriers"

    logger.info(f"Fetching client benefit rules for {client_code}, year {benefit_year}")

    query = f"""
        SELECT DISTINCT
            crd.ClientCode,
            crd.DataSource,
            crd.InsuranceCarrierID,
            crd.InsuranceHealthPlanID,
            crd.CardIssueCondition,
            crd.BenefitYear,
            crd.CardIssueStartDate,
            crd.CardIssueEndDate,
            ic.EnvironmentId
        FROM {rules_table} crd
        JOIN {carriers_table} ic ON crd.InsuranceCarrierID = ic.InsuranceCarrierID
        WHERE crd.IsCardIssuance = 'Y'
        AND crd.DataSource = 'ELIG'
        AND crd.ClientCode = '{client_code}'
        AND COALESCE(crd.isActive, 0) = 1
        AND crd.BenefitYear = {benefit_year}
        AND COALESCE(crd.RecordStatus, '') = 'A'
        AND COALESCE(crd.Affinity, '') = '{affinity}'
        AND CURRENT_DATE() BETWEEN crd.CardIssueStartDate AND crd.CardIssueEndDate
    """

    rules_df = spark.sql(query)
    count = rules_df.count()
    logger.info(f"Found {count} active benefit rules")

    return rules_df


def get_eligible_members_for_card_issue(
    spark: SparkSession,
    catalog: str,
    client_benefit_rules: DataFrame,
    plaid_members: DataFrame,
    date_params: Dict,
    client_code: str,
    benefit_year: int,
    affinity: str
) -> DataFrame:
    """
    Get members eligible for card issuance based on benefit and Plaid verification.

    Args:
        spark: SparkSession instance
        catalog: Catalog name
        client_benefit_rules: DataFrame with client benefit rules
        plaid_members: DataFrame with Plaid verified members
        date_params: Dictionary with date parameters
        client_code: Client code
        benefit_year: Benefit year
        affinity: Affinity value

    Returns:
        DataFrame with eligible members
    """
    mstr_elig_table = f"{catalog}.elig.mstr_elig_benefit_data"
    card_package_table = f"{catalog}.flex.card_package"
    card_mapping_table = f"{catalog}.otcfunds.benefit_card_mapping"
    card_load_ci_table = f"{catalog}.otcfunds.card_benefit_load_ci"

    eligible_date = date_params["eligible_as_of_date"].strftime("%Y-%m-%d")
    current_date = date_params["current_date"].strftime("%Y-%m-%d")

    logger.info(f"Fetching eligible members from {mstr_elig_table}")

    # Create temp views for joins
    client_benefit_rules.createOrReplaceTempView("client_benefit_rules_temp")
    plaid_members.createOrReplaceTempView("plaid_members_temp")

    query = f"""
        SELECT DISTINCT
            mstr.mstrEligID as MemberDataID,
            cc.ClientCode,
            mstr.NHLinkid,
            'CI' as RecordType,
            cc.DataSource,
            mstr.insCarrierID,
            mstr.insHealthPlanID,
            'OTC' as BenefitType,
            'Igloo' as BenefitSource,
            1 as isActive,
            'Y' as RequestToBeProcessed,
            mstr.LastName,
            CASE WHEN mstr.MiddleInitial IN ('-', '.', '0') THEN '' ELSE mstr.MiddleInitial END as MiddleInitial,
            mstr.FirstName,
            mstr.DOB,
            CASE WHEN COALESCE(mstr.MailingAddress1, '') = '' THEN mstr.Address1 ELSE mstr.MailingAddress1 END as MailingAddress1,
            CASE WHEN COALESCE(mstr.MailingAddress1, '') = '' THEN mstr.Address2 ELSE mstr.MailingAddress2 END as MailingAddress2,
            CASE WHEN COALESCE(mstr.MailingAddress1, '') = '' THEN mstr.City ELSE mstr.MailingCity END as MailingCity,
            CASE WHEN COALESCE(mstr.MailingAddress1, '') = '' THEN mstr.State ELSE mstr.MailingState END as MailingState,
            CASE WHEN COALESCE(mstr.MailingAddress1, '') = '' THEN REPLACE(mstr.ZipCode, '-', '') ELSE REPLACE(mstr.MailingZipCode, '-', '') END as MailingZipCode,
            'USA' as MailingCountryCode,
            mstr.HomePhoneNbr,
            NULL as BenefitCardNumber,
            mstr.FileTrackId,
            REPLACE(COALESCE(mstr.Language, ''), ',', '') as Language,
            mstr.DataSource as MSTR_DataSource,
            COALESCE(mstr.SSBCI, '') as SSBCI,
            RANK() OVER (PARTITION BY mstr.NHLinkid ORDER BY mstr.mstrEligID DESC) as Prank,
            cc.CardIssueCondition,
            cp.PackageID,
            cp.language as pkg_language,
            cp.language as CI_PackageLanguage,
            cc.EnvironmentId,
            m.NHMemberID,
            mstr.MasterMemberID
        FROM {mstr_elig_table} mstr
        JOIN client_benefit_rules_temp cc
            ON cc.InsuranceCarrierID = mstr.insCarrierID
            AND cc.InsuranceHealthPlanID = mstr.insHealthPlanID
        INNER JOIN plaid_members_temp m
            ON m.NHLinkID = mstr.NHLinkid
            AND m.InsuranceCarrierID = mstr.insCarrierID
        INNER JOIN {card_package_table} cp
            ON cp.NHLinkID = m.NHLinkid
            AND cp.NHMemberID = m.NHMemberID
            AND cp.InsuranceCarrierID = mstr.insCarrierID
            AND cp.InsuranceHealthPlanID = mstr.insHealthPlanID
            AND cp.ClientCode = '{client_code}'
            AND cp.BenefitYear = {benefit_year}
            AND cp.IsActive = 1
            AND cp.DataSource = 'elig'
        WHERE mstr.isActive = 1
        AND cp.PackageID IS NOT NULL
        AND (
            CAST('{eligible_date}' AS DATE) BETWEEN mstr.RecordEffDate AND mstr.RecordEndDate
            OR CAST('{current_date}' AS DATE) BETWEEN mstr.RecordEffDate AND mstr.RecordEndDate
        )
        AND (
            CAST('{eligible_date}' AS DATE) BETWEEN mstr.BenefitStartDate AND mstr.BenefitEndDate
            OR CAST('{current_date}' AS DATE) BETWEEN mstr.BenefitStartDate AND mstr.BenefitEndDate
        )
        AND mstr.BenefitEndDate > mstr.BenefitStartDate
        AND NOT EXISTS (
            SELECT 1
            FROM {card_mapping_table} bcm
            WHERE bcm.NHLinkID = mstr.NHLinkid
            AND bcm.InsCarrierID = mstr.InsCarrierID
            AND COALESCE(bcm.IsActive, 0) = 1
            AND COALESCE(bcm.Affinity, '') = '{affinity}'
        )
        AND CASE WHEN COALESCE(mstr.MailingAddress1, '') = '' THEN mstr.Address1 ELSE mstr.MailingAddress1 END != ''
        AND NOT EXISTS (
            SELECT 1
            FROM {card_load_ci_table} cl
            WHERE cl.NHLinkID = mstr.NHLinkid
            AND cl.RecordType = 'CI'
            AND cl.ClientCode = '{client_code}'
            AND COALESCE(cl.IsActive, 0) = 1
            AND COALESCE(cl.Affinity, '') = '{affinity}'
            AND cl.BenefitSource = 'Igloo'
            AND (
                (cl.RequestRecordStatus = 'SUCCESS' AND cl.ResponseRecordStatus IS NULL)
                OR (cl.RequestRecordStatus = 'SUCCESS' AND cl.ResponseRecordStatus = 'SUCCESS')
                OR (cl.RequestRecordStatus IS NULL AND cl.RequestToBeProcessed = 'Y')
            )
        )
    """

    eligible_df = spark.sql(query)
    count = eligible_df.count()
    logger.info(f"Found {count} eligible members for card issuance")

    return eligible_df


def apply_card_issue_business_rules(
    spark: SparkSession,
    catalog: str,
    eligible_members: DataFrame,
    affinity: str
) -> DataFrame:
    """
    Apply business rules and filters for card issuance.

    Args:
        spark: SparkSession instance
        catalog: Catalog name
        eligible_members: DataFrame with eligible members
        affinity: Affinity value

    Returns:
        Filtered DataFrame
    """
    logger.info("Applying business rules for card issuance")

    # Remove duplicates (keep only highest rank per NHLinkid)
    filtered = eligible_members.filter(col("Prank") == 1)

    # Remove member exceptions
    member_exception_table = f"{catalog}.otcfunds.flexbenefit_member_exception"

    exceptions = spark.sql(f"""
        SELECT NHLinkID, CarrierID
        FROM {member_exception_table}
        WHERE isactive = 1
        AND CURRENT_DATE() BETWEEN RecordEffDate AND RecordEndDate
    """)

    filtered = filtered.join(
        exceptions,
        (filtered.NHLinkid == exceptions.NHLinkID) &
        (filtered.insCarrierID == exceptions.CarrierID),
        "left_anti"
    )

    # Filter for CardIssueCondition = 'ALL' and NHMemberID is not null
    filtered = filtered.filter(
        (col("CardIssueCondition") == "ALL") &
        (col("NHMemberID").isNotNull())
    )

    count = filtered.count()
    logger.info(f"After business rules: {count} members")

    return filtered


def apply_load_threshold(
    spark: SparkSession,
    catalog: str,
    members_df: DataFrame,
    client_code: str,
    benefit_year: int,
    affinity: str
) -> DataFrame:
    """
    Apply load threshold limit to member records.

    Args:
        spark: SparkSession instance
        catalog: Catalog name
        members_df: DataFrame with member records
        client_code: Client code
        benefit_year: Benefit year
        affinity: Affinity value

    Returns:
        DataFrame limited by threshold
    """
    file_info_table = f"{catalog}.otcfunds.file_info"

    threshold_query = f"""
        SELECT COALESCE(LoadThreshold, 0) as LoadThreshold
        FROM {file_info_table}
        WHERE ClientCode = '{client_code}'
        AND isActive = 1
        AND Direction = 'OUT'
        AND SnapshotFlag = 'CI'
        AND BenefitYear = {benefit_year}
        AND COALESCE(Affinity, '') = '{affinity}'
    """

    threshold_result = spark.sql(threshold_query).first()
    load_threshold = threshold_result["LoadThreshold"] if threshold_result else 0

    if load_threshold > 0:
        logger.info(f"Applying load threshold: {load_threshold}")
        return members_df.limit(load_threshold)
    else:
        logger.info("No load threshold configured, processing all records")
        return members_df


def insert_benefit_card_mapping(
    spark: SparkSession,
    catalog: str,
    members_df: DataFrame,
    benefit_year: int,
    affinity: str
) -> int:
    """
    Insert records into BenefitCardMapping table.

    Args:
        spark: SparkSession instance
        catalog: Catalog name
        members_df: DataFrame with member records
        benefit_year: Benefit year
        affinity: Affinity value

    Returns:
        Number of records inserted
    """
    mapping_table = f"{catalog}.otcfunds.benefit_card_mapping"

    logger.info(f"Inserting records into {mapping_table}")

    mapping_df = members_df.select(
        col("MemberDataID").alias("mstrEligID"),
        col("NHLinkid").alias("NHLinkID"),
        col("insCarrierID").alias("InsCarrierID"),
        col("insHealthPlanID").alias("InsHealthPlanID"),
        lit(None).alias("BenefitCardNumber"),
        col("MSTR_DataSource").alias("DataSource"),
        col("ClientCode"),
        current_timestamp().alias("CreateDate"),
        lit("system").alias("CreateUser"),
        lit(benefit_year).alias("BenefitYear"),
        lit(affinity).alias("Affinity"),
        col("NHMemberID"),
        col("EnvironmentId").alias("EnvironmentID")
    ).distinct()

    # Anti-join to avoid duplicates
    existing_mappings = spark.sql(f"""
        SELECT mstrEligID, NHLinkID, InsCarrierID
        FROM {mapping_table}
        WHERE COALESCE(IsActive, 0) = 1
        AND COALESCE(Affinity, '') = '{affinity}'
    """)

    mapping_df = mapping_df.join(
        existing_mappings,
        (mapping_df.mstrEligID == existing_mappings.mstrEligID) &
        (mapping_df.NHLinkID == existing_mappings.NHLinkID) &
        (mapping_df.InsCarrierID == existing_mappings.InsCarrierID),
        "left_anti"
    )

    record_count = mapping_df.count()

    if record_count > 0:
        # Write to table
        mapping_df.write.mode("append").saveAsTable(mapping_table)
        logger.info(f"Inserted {record_count} records into BenefitCardMapping")
    else:
        logger.info("No new records to insert into BenefitCardMapping")

    return record_count


def insert_card_benefit_load_ci(
    spark: SparkSession,
    catalog: str,
    members_df: DataFrame,
    benefit_year: int,
    affinity: str
) -> int:
    """
    Insert records into CardBenefitLoad_CI table with TrackingID from physical table.

    Args:
        spark: SparkSession instance
        catalog: Catalog name
        members_df: DataFrame with member records
        benefit_year: Benefit year
        affinity: Affinity value

    Returns:
        Number of records inserted
    """
    ci_table = f"{catalog}.otcfunds.card_benefit_load_ci"
    mapping_table = f"{catalog}.otcfunds.benefit_card_mapping"

    logger.info(f"Inserting records into {ci_table}")

    # Prepare final dataframe with all required fields
    final_df = members_df.select(
        col("MemberDataID"),
        col("ClientCode"),
        col("NHLinkid").alias("NHLinkID"),
        col("RecordType"),
        col("DataSource").alias("MemberDataSource"),
        col("insCarrierID").alias("InsCarrierID"),
        col("insHealthPlanID").alias("InsHealthPlanID"),
        col("BenefitType"),
        col("BenefitSource"),
        col("isActive").alias("IsActive"),
        col("RequestToBeProcessed"),
        col("LastName"),
        col("MiddleInitial"),
        col("FirstName"),
        col("DOB"),
        col("MailingAddress1"),
        col("MailingAddress2"),
        col("MailingCity"),
        col("MailingState"),
        col("MailingZipCode"),
        col("MailingCountryCode").alias("MailingCountry"),
        col("HomePhoneNbr"),
        col("BenefitCardNumber"),
        col("Language"),
        col("PackageID"),
        lit("").alias("CarrierMessage"),
        lit(benefit_year).alias("BenefitYear"),
        col("CI_PackageLanguage").alias("PackageLanguage"),
        lit(affinity).alias("Affinity"),
        col("EnvironmentId").alias("EnvironmentID"),
        lit("").alias("CaregiverName"),
        lit("USPS").alias("FulfillmentCarrier"),
        lit("First Class").alias("FulfillmentShippingType"),
        lit(None).alias("Track2UID"),
        lit("").alias("VariableCarrierMessageData"),
        lit("CREATE").alias("Action"),
        lit("").alias("isCardVirtual"),
        col("NHMemberID")
    )

    # Join with BenefitCardMapping to get BenefitCardMappingID
    mapping_ids = spark.sql(f"""
        SELECT
            BenefitCardMappingID,
            mstrEligID,
            NHLinkID,
            InsCarrierID,
            InsHealthPlanID
        FROM {mapping_table}
        WHERE COALESCE(IsActive, 0) = 1
        AND COALESCE(Affinity, '') = '{affinity}'
    """)

    final_df = final_df.join(
        mapping_ids,
        (final_df.MemberDataID == mapping_ids.mstrEligID) &
        (final_df.NHLinkID == mapping_ids.NHLinkID) &
        (final_df.InsCarrierID == mapping_ids.InsCarrierID) &
        (final_df.InsHealthPlanID == mapping_ids.InsHealthPlanID),
        "inner"
    ).drop(
        mapping_ids.mstrEligID,
        mapping_ids.NHLinkID,
        mapping_ids.InsCarrierID,
        mapping_ids.InsHealthPlanID
    )

    record_count = final_df.count()

    if record_count > 0:
        # Get starting TrackingID from physical table
        start_tracking_id = get_next_tracking_ids(spark, catalog, record_count)

        # Add row number and TrackingID column
        window_spec = Window.orderBy(monotonically_increasing_id())
        final_df = final_df.withColumn("row_num", row_number().over(window_spec))
        final_df = final_df.withColumn("TrackingID", col("row_num") + lit(start_tracking_id - 1))
        final_df = final_df.drop("row_num")

        # Write to table
        final_df.write.mode("append").saveAsTable(ci_table)
        logger.info(f"Inserted {record_count} records into CardBenefitLoad_CI with TrackingIDs {start_tracking_id} to {start_tracking_id + record_count - 1}")
    else:
        logger.info("No records to insert into CardBenefitLoad_CI")

    return record_count
