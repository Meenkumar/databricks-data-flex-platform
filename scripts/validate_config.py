#!/usr/bin/env python3
"""
Configuration validation script.

Validates YAML configuration files for syntax and required fields.
"""
import sys
import argparse
from pathlib import Path
import yaml
from typing import Dict, List


class ConfigValidator:
    """Validates configuration files."""

    def __init__(self, environment: str):
        self.environment = environment
        self.errors = []
        self.warnings = []

    def validate_yaml_file(self, file_path: Path) -> bool:
        """Validate YAML file syntax."""
        try:
            with open(file_path, 'r') as f:
                yaml.safe_load(f)
            print(f"✓ Valid YAML: {file_path}")
            return True
        except yaml.YAMLError as e:
            self.errors.append(f"Invalid YAML in {file_path}: {e}")
            return False

    def validate_schema_config(self, schema_file: Path) -> bool:
        """Validate schema configuration."""
        try:
            with open(schema_file, 'r') as f:
                config = yaml.safe_load(f)

            required_fields = ['schema_name', 'business_keys', 'required_columns']

            for field in required_fields:
                if field not in config:
                    self.errors.append(
                        f"Missing required field '{field}' in {schema_file}"
                    )
                    return False

            print(f"✓ Valid schema config: {schema_file.name}")
            return True

        except Exception as e:
            self.errors.append(f"Error validating {schema_file}: {e}")
            return False

    def validate_job_config(self, job_file: Path) -> bool:
        """Validate job configuration."""
        try:
            with open(job_file, 'r') as f:
                config = yaml.safe_load(f)

            required_fields = ['name', 'tasks', 'job_clusters']

            for field in required_fields:
                if field not in config:
                    self.errors.append(
                        f"Missing required field '{field}' in {job_file}"
                    )
                    return False

            # Validate tasks
            if not config['tasks']:
                self.errors.append(f"No tasks defined in {job_file}")
                return False

            print(f"✓ Valid job config: {job_file.name}")
            return True

        except Exception as e:
            self.errors.append(f"Error validating {job_file}: {e}")
            return False

    def validate_all(self) -> bool:
        """Validate all configuration files."""
        print(f"\nValidating configurations for environment: {self.environment}\n")

        config_dir = Path("config")
        resources_dir = Path("resources")

        # Validate schema files
        print("Validating schema configurations...")
        schema_dir = config_dir / "schemas"
        if schema_dir.exists():
            for schema_file in schema_dir.glob("*.yml"):
                self.validate_schema_config(schema_file)

        # Validate job files
        print("\nValidating job configurations...")
        job_dir = resources_dir / "jobs"
        if job_dir.exists():
            for job_file in job_dir.glob("*.yml"):
                self.validate_job_config(job_file)

        # Validate pipeline files
        print("\nValidating pipeline configurations...")
        pipeline_dir = resources_dir / "pipelines"
        if pipeline_dir.exists():
            for pipeline_file in pipeline_dir.glob("*.yml"):
                self.validate_yaml_file(pipeline_file)

        # Print results
        print("\n" + "="*60)
        if self.errors:
            print(f"❌ Validation FAILED with {len(self.errors)} error(s):\n")
            for error in self.errors:
                print(f"  • {error}")
            return False
        else:
            print("✅ All validations PASSED")
            if self.warnings:
                print(f"\n⚠️  {len(self.warnings)} warning(s):")
                for warning in self.warnings:
                    print(f"  • {warning}")
            return True


def main():
    parser = argparse.ArgumentParser(description='Validate configuration files')
    parser.add_argument(
        '--environment',
        choices=['dev', 'qa', 'prod'],
        default='dev',
        help='Environment to validate'
    )

    args = parser.parse_args()

    validator = ConfigValidator(args.environment)
    success = validator.validate_all()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
