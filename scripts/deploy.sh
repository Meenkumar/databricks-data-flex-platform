#!/bin/bash
###############################################################################
# Deployment Script for Databricks Data Platform
#
# Usage: ./deploy.sh <environment> [options]
#
# Environments: dev, qa, prod
# Options:
#   --validate-only    Only validate configuration
#   --skip-tests       Skip running tests before deployment
###############################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
ENVIRONMENT=${1:-dev}
VALIDATE_ONLY=false
SKIP_TESTS=false

for arg in "$@"; do
    case $arg in
        --validate-only)
            VALIDATE_ONLY=true
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
    esac
done

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Databricks Data Platform Deployment${NC}"
echo -e "${GREEN}Environment: ${ENVIRONMENT}${NC}"
echo -e "${GREEN}========================================${NC}"

# Validate environment
if [[ ! "$ENVIRONMENT" =~ ^(dev|qa|prod)$ ]]; then
    echo -e "${RED}Error: Invalid environment. Must be dev, qa, or prod${NC}"
    exit 1
fi

# Check if databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}Error: Databricks CLI not found. Please install it first.${NC}"
    exit 1
fi

# Validate configuration
echo -e "\n${YELLOW}Step 1: Validating configuration...${NC}"
python scripts/validate_config.py --environment "$ENVIRONMENT"

if [ "$VALIDATE_ONLY" = true ]; then
    echo -e "${GREEN}Validation complete. Exiting (validate-only mode).${NC}"
    exit 0
fi

# Run tests
if [ "$SKIP_TESTS" = false ]; then
    echo -e "\n${YELLOW}Step 2: Running tests...${NC}"
    pytest tests/unit -v
else
    echo -e "\n${YELLOW}Step 2: Skipping tests (--skip-tests flag)${NC}"
fi

# Sync source code
echo -e "\n${YELLOW}Step 3: Syncing source code to Databricks workspace...${NC}"
databricks workspace import-dir src /Workspace/src --overwrite

# Sync notebooks
echo -e "\n${YELLOW}Step 4: Syncing notebooks...${NC}"
databricks workspace import-dir notebooks /Workspace/notebooks --overwrite

# Sync SQL scripts
echo -e "\n${YELLOW}Step 5: Syncing SQL scripts...${NC}"
databricks workspace import-dir sql /Workspace/sql --overwrite

# Deploy jobs
echo -e "\n${YELLOW}Step 6: Deploying job workflows...${NC}"
for job_file in resources/jobs/*.yml; do
    job_name=$(basename "$job_file" .yml)
    echo "Deploying job: $job_name"
    databricks jobs create --json-file "$job_file" || \
    databricks jobs update --json-file "$job_file"
done

# Deploy DLT pipelines
echo -e "\n${YELLOW}Step 7: Deploying Delta Live Tables pipelines...${NC}"
for pipeline_file in resources/pipelines/*.yml; do
    pipeline_name=$(basename "$pipeline_file" .yml)
    echo "Deploying pipeline: $pipeline_name"
    databricks pipelines create --json-file "$pipeline_file" || \
    databricks pipelines update --json-file "$pipeline_file"
done

# Run database migrations
echo -e "\n${YELLOW}Step 8: Running database migrations...${NC}"
for migration_file in sql/migrations/*.sql; do
    if [ -f "$migration_file" ]; then
        echo "Applying migration: $(basename $migration_file)"
        databricks workspace import "$migration_file" "/Workspace/migrations/$(basename $migration_file)" --overwrite
    fi
done

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}Environment: ${ENVIRONMENT}${NC}"
echo -e "${GREEN}========================================${NC}"

# Print next steps
echo -e "\n${YELLOW}Next Steps:${NC}"
echo "1. Verify jobs in Databricks UI"
echo "2. Run smoke tests: pytest tests/integration -v"
echo "3. Monitor first pipeline execution"
