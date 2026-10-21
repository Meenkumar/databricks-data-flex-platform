"""
Setup script for Databricks Data Platform.
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

setup(
    name="databricks-data-platform",
    version="1.0.0",
    author="Data Engineering Team",
    author_email="data-engineering@company.com",
    description="Production-ready Databricks data platform for healthcare data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/databricks-data-platform",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=3.4.0",
        "delta-spark>=2.4.0",
        "pyyaml>=6.0",
        "paramiko>=3.0.0",
        "requests>=2.28.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-mock>=3.10.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "isort>=5.12.0",
            "pre-commit>=3.0.0",
        ],
    },
    entry_points={
        'console_scripts': [
            'dp-validate=scripts.validate_config:main',
        ],
    },
    include_package_data=True,
    zip_safe=False,
)
