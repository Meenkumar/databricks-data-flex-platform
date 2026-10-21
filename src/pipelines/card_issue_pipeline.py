"""
Card Issue Pipeline - Igloo card issuance data processing.
"""
from pyspark.sql import SparkSession
from common.logging import get_logger
from common.config_loader import ConfigLoader
from common.audit import AuditLogger
from common.date_utils import calculate_card_issue_dates
from common.plaid_verification import check_plaid_verification_status
from common.card_issue_functions import (
    get_client_benefit_rules,
    get_eligible_members_for_card_issue,
    apply_card_issue_business_rules,
    apply_load_threshold,
    insert_benefit_card_mapping,
    insert_card_benefit_load_ci
)
from common.failure_tracking import (
    create_failure_tracking_table,
    clear_previous_run_failures,
    track_plaid_failures,
    track_card_package_failures,
    track_member_validation_failures,
    get_failure_summary
)
import uuid
from datetime import datetime


logger = get_logger(__name__)


class CardIssuePipeline:
    """Pipeline for processing card issue data."""

    def __init__(self, spark: SparkSession, environment: str = "dev"):
        """
        Initialize card issue pipeline.

        Args:
            spark: SparkSession instance
            environment: Environment name (dev/qa/prod)
        """
        self.spark = spark
        self.environment = environment
        self.config_loader = ConfigLoader()
        self.audit_logger = AuditLogger(spark)

        # Load pipeline configuration
        self.config = self.config_loader.load_config(f"card_issue_pipeline_{environment}")

        # Set catalog and database
        self.catalog = self.config.get("catalog", "data_nonpii_dev")
        self.database = self.config.get("database", "flex")

        # Get parameters from config
        self.client_code = self.config.get("client_code", "H225")
        self.benefit_year = self.config.get("benefit_year", 2026)
        self.affinity = self.config.get("affinity", "Igloo")
        self.date_offset_days = self.config.get("date_offset_days", 30)

        logger.info(
            f"Initialized Card Issue Pipeline: catalog={self.catalog}, "
            f"client_code={self.client_code}, benefit_year={self.benefit_year}, "
            f"affinity={self.affinity}"
        )

    def run(self):
        """Execute the card issue pipeline."""
        audit_id = None
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        processing_date = datetime.now()

        try:
            # Initialize failure tracking table
            create_failure_tracking_table(
                spark=self.spark,
                catalog=self.catalog,
                database=self.database
            )

            # Clear previous run failures for this configuration
            clear_previous_run_failures(
                spark=self.spark,
                catalog=self.catalog,
                database=self.database,
                client_code=self.client_code,
                benefit_year=self.benefit_year,
                affinity=self.affinity,
                processing_date=processing_date
            )

            # Start audit logging
            audit_id = self.audit_logger.log_pipeline_start(
                pipeline_name="card_issue_pipeline",
                source_system="eligibility",
                target_table=f"{self.catalog}.otcfunds.card_benefit_load_ci"
            )

            logger.info(f"Starting Card Issue Pipeline - RunID: {run_id}", audit_id=audit_id)

            # Step 1: Calculate date parameters using common function
            date_params = calculate_card_issue_dates(
                benefit_year=self.benefit_year,
                offset_days=self.date_offset_days
            )
            logger.info(
                f"Date parameters: processing_date={date_params['processing_date']}, "
                f"eligible_as_of_date={date_params['eligible_as_of_date']}"
            )

            # Step 2: Get client benefit rules using common function
            client_benefit_rules = get_client_benefit_rules(
                spark=self.spark,
                catalog=self.catalog,
                client_code=self.client_code,
                benefit_year=self.benefit_year,
                affinity=self.affinity
            )
            rules_count = client_benefit_rules.count()
            logger.info(f"Loaded {rules_count} client benefit rules")

            if rules_count == 0:
                logger.warning("No active client benefit rules found. Exiting pipeline.")
                return

            # Step 3: Get Plaid verified members using common function
            plaid_requests_table = f"{self.catalog}.{self.database}.plaid_member_requests"
            plaid_responses_table = f"{self.catalog}.{self.database}.plaid_member_responses"

            # Get carrier IDs from benefit rules
            carrier_ids = [row.InsuranceCarrierID for row in client_benefit_rules.select("InsuranceCarrierID").distinct().collect()]

            # Track Plaid failures
            plaid_failure_count = track_plaid_failures(
                spark=self.spark,
                catalog=self.catalog,
                database=self.database,
                plaid_requests_table=plaid_requests_table,
                plaid_responses_table=plaid_responses_table,
                carrier_ids=carrier_ids,
                client_code=self.client_code,
                benefit_year=self.benefit_year,
                affinity=self.affinity,
                run_id=run_id,
                processing_date=processing_date
            )
            logger.info(f"Tracked {plaid_failure_count} Plaid verification failures")

            plaid_members = check_plaid_verification_status(
                spark=self.spark,
                plaid_requests_table=plaid_requests_table,
                plaid_responses_table=plaid_responses_table,
                carrier_ids=carrier_ids
            )
            plaid_count = plaid_members.count()
            logger.info(f"Found {plaid_count} Plaid verified members")

            if plaid_count == 0:
                logger.warning("No Plaid verified members found. Exiting pipeline.")
                # Show failure summary before exiting
                failure_summary = get_failure_summary(
                    spark=self.spark,
                    catalog=self.catalog,
                    database=self.database,
                    client_code=self.client_code,
                    benefit_year=self.benefit_year,
                    affinity=self.affinity,
                    processing_date=processing_date
                )
                failure_summary.show(truncate=False)
                return

            # Step 4: Get eligible members for card issuance using common function
            # First, get all members who should be eligible (before card package check)
            mstr_elig_table = f"{self.catalog}.elig.mstr_elig_benefit_data"

            # Create temp view for pre-eligibility check
            client_benefit_rules.createOrReplaceTempView("cbr_temp")
            plaid_members.createOrReplaceTempView("plaid_temp")

            pre_eligible_query = f"""
            SELECT DISTINCT
                mstr.mstrEligID as MemberDataID,
                mstr.NHLinkid,
                mstr.insCarrierID,
                mstr.insHealthPlanID,
                m.NHMemberID
            FROM {mstr_elig_table} mstr
            JOIN cbr_temp cc
                ON cc.InsuranceCarrierID = mstr.insCarrierID
                AND cc.InsuranceHealthPlanID = mstr.insHealthPlanID
            INNER JOIN plaid_temp m
                ON m.NHLinkID = mstr.NHLinkid
                AND m.InsuranceCarrierID = mstr.insCarrierID
            WHERE mstr.isActive = 1
            """
            pre_eligible_df = self.spark.sql(pre_eligible_query)

            # Track card package failures
            card_package_table = f"{self.catalog}.{self.database}.card_package"
            package_failure_count = track_card_package_failures(
                spark=self.spark,
                catalog=self.catalog,
                database=self.database,
                eligible_members=pre_eligible_df,
                card_package_table=card_package_table,
                client_code=self.client_code,
                benefit_year=self.benefit_year,
                affinity=self.affinity,
                run_id=run_id,
                processing_date=processing_date
            )
            logger.info(f"Tracked {package_failure_count} card package failures")

            eligible_members = get_eligible_members_for_card_issue(
                spark=self.spark,
                catalog=self.catalog,
                client_benefit_rules=client_benefit_rules,
                plaid_members=plaid_members,
                date_params=date_params,
                client_code=self.client_code,
                benefit_year=self.benefit_year,
                affinity=self.affinity
            )
            eligible_count = eligible_members.count()
            logger.info(f"Found {eligible_count} eligible members for card issuance")

            if eligible_count == 0:
                logger.info("No eligible members found. Pipeline completed.")
                # Show failure summary
                failure_summary = get_failure_summary(
                    spark=self.spark,
                    catalog=self.catalog,
                    database=self.database,
                    client_code=self.client_code,
                    benefit_year=self.benefit_year,
                    affinity=self.affinity,
                    processing_date=processing_date
                )
                failure_summary.show(truncate=False)
                return

            # Step 5: Apply business rules and filters using common function
            # Cache eligible members before validation for failure tracking
            eligible_members.cache()

            filtered_members = apply_card_issue_business_rules(
                spark=self.spark,
                catalog=self.catalog,
                eligible_members=eligible_members,
                affinity=self.affinity
            )
            filtered_count = filtered_members.count()
            logger.info(f"After business rules: {filtered_count} members")

            # Track business rule failures
            business_rule_failures = track_member_validation_failures(
                spark=self.spark,
                catalog=self.catalog,
                database=self.database,
                before_validation_df=eligible_members,
                after_validation_df=filtered_members,
                failure_stage="BUSINESS_RULES",
                client_code=self.client_code,
                benefit_year=self.benefit_year,
                affinity=self.affinity,
                run_id=run_id,
                processing_date=processing_date
            )
            logger.info(f"Tracked {business_rule_failures} business rule failures")

            # Unpersist cached dataframe
            eligible_members.unpersist()

            if filtered_count == 0:
                logger.info("No members passed business rules. Pipeline completed.")
                # Show failure summary
                failure_summary = get_failure_summary(
                    spark=self.spark,
                    catalog=self.catalog,
                    database=self.database,
                    client_code=self.client_code,
                    benefit_year=self.benefit_year,
                    affinity=self.affinity,
                    processing_date=processing_date
                )
                failure_summary.show(truncate=False)
                return

            # Step 6: Apply load threshold using common function
            # Cache filtered members for threshold tracking
            filtered_members.cache()

            final_members = apply_load_threshold(
                spark=self.spark,
                catalog=self.catalog,
                members_df=filtered_members,
                client_code=self.client_code,
                benefit_year=self.benefit_year,
                affinity=self.affinity
            )
            final_count = final_members.count()
            logger.info(f"Final member count (after threshold): {final_count}")

            # Track load threshold exclusions
            if final_count < filtered_count:
                threshold_failures = track_member_validation_failures(
                    spark=self.spark,
                    catalog=self.catalog,
                    database=self.database,
                    before_validation_df=filtered_members,
                    after_validation_df=final_members,
                    failure_stage="LOAD_THRESHOLD",
                    client_code=self.client_code,
                    benefit_year=self.benefit_year,
                    affinity=self.affinity,
                    run_id=run_id,
                    processing_date=processing_date
                )
                logger.info(f"Tracked {threshold_failures} load threshold exclusions")

            # Unpersist cached dataframe
            filtered_members.unpersist()

            if final_count == 0:
                logger.info("No members after threshold. Pipeline completed.")
                # Show failure summary
                failure_summary = get_failure_summary(
                    spark=self.spark,
                    catalog=self.catalog,
                    database=self.database,
                    client_code=self.client_code,
                    benefit_year=self.benefit_year,
                    affinity=self.affinity,
                    processing_date=processing_date
                )
                failure_summary.show(truncate=False)
                return

            # Step 7: Insert into BenefitCardMapping using common function
            mapping_inserts = insert_benefit_card_mapping(
                spark=self.spark,
                catalog=self.catalog,
                members_df=final_members,
                benefit_year=self.benefit_year,
                affinity=self.affinity
            )
            logger.info(f"Inserted {mapping_inserts} records into BenefitCardMapping")

            # Step 8: Insert into CardBenefitLoad_CI using common function
            ci_inserts = insert_card_benefit_load_ci(
                spark=self.spark,
                catalog=self.catalog,
                members_df=final_members,
                benefit_year=self.benefit_year,
                affinity=self.affinity
            )
            logger.info(f"Inserted {ci_inserts} records into CardBenefitLoad_CI")

            # Generate and display failure summary
            logger.info("Generating failure summary for dashboard...")
            failure_summary = get_failure_summary(
                spark=self.spark,
                catalog=self.catalog,
                database=self.database,
                client_code=self.client_code,
                benefit_year=self.benefit_year,
                affinity=self.affinity,
                processing_date=processing_date
            )

            failure_summary_count = failure_summary.count()
            if failure_summary_count > 0:
                logger.info(f"Total failure categories tracked: {failure_summary_count}")
                failure_summary.show(truncate=False)
            else:
                logger.info("No failures tracked in this run")

            # Complete audit logging
            self.audit_logger.log_pipeline_complete(
                audit_id=audit_id,
                records_read=eligible_count,
                records_written=ci_inserts,
                metadata={
                    "run_id": run_id,
                    "eligible_members": eligible_count,
                    "filtered_members": filtered_count,
                    "final_members": final_count,
                    "mapping_inserts": mapping_inserts,
                    "ci_inserts": ci_inserts,
                    "plaid_failures": plaid_failure_count,
                    "package_failures": package_failure_count,
                    "business_rule_failures": business_rule_failures,
                    "failure_categories": failure_summary_count
                }
            )

            logger.info(f"Card Issue Pipeline completed successfully - RunID: {run_id}")

        except Exception as e:
            if audit_id:
                self.audit_logger.log_pipeline_failure(
                    audit_id=audit_id,
                    error_message=str(e)
                )
            logger.error(f"Card Issue Pipeline failed: {str(e)}")
            raise


def main():
    """Main entry point for the pipeline."""
    spark = SparkSession.builder \
        .appName("Card Issue Pipeline") \
        .getOrCreate()

    import sys
    environment = sys.argv[1] if len(sys.argv) > 1 else "dev"

    pipeline = CardIssuePipeline(spark, environment)
    pipeline.run()


if __name__ == "__main__":
    main()
