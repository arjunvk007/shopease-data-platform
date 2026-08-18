# ShopEase Data Platform

A Databricks Asset Bundle project that implements the ShopEase e-commerce data platform using a bronze / silver / gold medallion architecture, deployed and orchestrated with Databricks Workflows and GitHub Actions CI/CD.

## Overview

- Bronze - raw ingestion of source data such as orders, customers, products, and clickstream events into Delta tables with minimal transformation.
- Silver - cleaned, deduplicated, and conformed data with enforced schemas and data quality checks.
- Gold - business-level aggregates and marts such as daily sales and customer lifetime value, ready for BI and analytics consumption.

## Project layout

    shopease-data-platform/
    |-- databricks.yml              # Root bundle configuration
    |-- resources/
    |   |-- shopease_job.job.yml    # Job definition: bronze, silver, gold
    |-- src/
    |   |-- bronze/                 # Raw ingestion scripts
    |   |-- silver/                 # Cleansing and conformance transforms
    |   |-- gold/                   # Business aggregates and marts
    |   |-- pipeline/               # Local orchestration helper
    |-- tests/                      # Unit tests for transformation logic
    |-- .github/workflows/          # CI and deploy pipelines

## Getting started

### Prerequisites

- Databricks CLI v0.218 or later
- A Databricks workspace with a configured profile via databricks configure
- Python 3.10 or later

### Validate the bundle

    databricks bundle validate -t dev

### Deploy to a target environment

    databricks bundle deploy -t dev

### Run the job

    databricks bundle run shopease_job -t dev

## CI/CD

- .github/workflows/ci.yml runs on every pull request: lints and unit-tests the Python source under src and tests, then validates the bundle.
- .github/workflows/deploy.yml runs on pushes to main: deploys the bundle to the target Databricks workspace using databricks bundle deploy.

## License

Internal project, no license specified yet.
