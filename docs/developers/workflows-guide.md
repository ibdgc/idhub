# GitHub Actions Workflows Guide

This guide provides an overview of the GitHub Actions workflows used in the IDhub project for continuous integration, deployment, and data operations.

## Overview

The project uses a series of workflows to automate testing, deployment, and routine data management tasks. These workflows are defined as YAML files in the `.github/workflows` directory.

!!! info "General Philosophy"
    The general philosophy is:

    -   **CI on every push/PR**: All code is tested automatically.
    -   **CD for key branches**: Pushes to `qa` and `prod` trigger deployments.
    -   **Manual triggers for sensitive operations**: Data ingestion and direct deployments can be triggered manually by authorized users.
    -   **Scheduled tasks for routine syncs**: The REDCap sync runs on a schedule to keep data fresh.

## Continuous Integration (CI)

!!! abstract "Workflow: `test-and-coverage.yml`"
    This is the primary CI workflow that ensures code quality and correctness.

    -   **Trigger**: Runs on any push or pull request to the `main`, `develop`, `prod`, or `qa` branches.
    -   **Purpose**: To run the test suite for each microservice and upload code coverage reports.

    !!! tip "Workflow Diagram"
        ```mermaid
        graph TD
            A[Push or PR] --> B{Branch Check};
            B -->|main, dev, etc.| C[Start 'test' Job];
            
            subgraph "Matrix Job: test"
                direction LR
                D[gsid-service]
                E[redcap-pipeline]
                F[fragment-validator]
                G[table-loader]
            end

            C --> D & E & F & G;

            D --> H[Build, Test, Upload Coverage];
            E --> I[Build, Test, Upload Coverage];
            F --> J[Build, Test, Upload Coverage];
            G --> K[Build, Test, Upload Coverage];

            K -- needs --> L[Start 'coverage-summary' Job];
            L --> M[Download Artifacts & Post Summary];
        ```

    !!! info "Key Steps"
        1.  **Matrix Strategy**: The `test` job runs in parallel for each of the four main services: `gsid-service`, `redcap-pipeline`, `fragment-validator`, and `table-loader`.
        2.  **Build & Test**: For each service, it builds a dedicated test container using the `docker-compose.test.yml` file and runs the tests within that container.
        3.  **Upload Artifacts**: It uploads the generated HTML coverage reports and JUnit test reports as artifacts. This allows developers to download and inspect test results and coverage locally.
        4.  **Codecov Upload**: Coverage reports in XML format are uploaded to Codecov for tracking and analysis.
        5.  **Coverage Summary**: A final job runs after all tests are complete, downloads the coverage artifacts, and posts a summary to the GitHub job summary page, making it easy to see the coverage for each service at a glance.

## Continuous Deployment (CD)

!!! abstract "Workflow: `deploy.yml`"
    This workflow handles deploying the entire application stack to the `qa` and `prod` environments.

    -   **Trigger**:
        -   Automatically on pushes to the `qa` and `prod` branches.
        -   Manually via a `workflow_dispatch` event, allowing a user to choose the target environment.
    -   **Purpose**: To securely connect to the target server, set up the environment, and restart the application services using Docker Compose.

    !!! info "Key Steps"
        1.  **Determine Environment**: The job first determines if it's deploying to `qa` or `prod` based on the branch name or the manual input.
        2.  **Set up SSH**: It uses a secret SSH key to establish a secure connection to the deployment server.
        3.  **Create `.env` file**: An environment-specific `.env.deploy` file is created locally using secrets stored in GitHub. This file contains all necessary environment variables for the application.
        4.  **Deploy to Server**:
            -   The `.env.deploy` file is securely copied to `/opt/idhub/.env` on the server.
            -   An SSH command is executed on the server to:
                -   Pull the latest code from the correct branch.
                -   Stop the running services.
                -   Rebuild the Docker images for the services.
                -   Restart the services using `docker-compose up -d`.
                -   Run health checks to ensure the services started correctly.
        5.  **Cleanup**: The local SSH key is removed.

## Documentation Workflow

!!! abstract "Workflow: `docs.yml`"
    This workflow automates the build and deployment of this documentation site.

    -   **Trigger**: Runs on pushes to the `main` branch that modify files in the `docs/` directory or the `mkdocs.yml` file.
    -   **Purpose**: To build the MkDocs site and deploy it to GitHub Pages.

    !!! info "Key Steps"
        1.  **Build**: It installs the Python dependencies (including MkDocs and the Material theme) and runs the `mkdocs build` command to generate the static HTML site.
        2.  **Upload Artifact**: The generated `site/` directory is uploaded as a Pages artifact.
        3.  **Deploy**: A second job, `deploy`, uses the `actions/deploy-pages@v4` action to deploy the artifact to the `github-pages` environment.

## Data & Operational Workflows

!!! abstract "Workflow: `fragment-ingestion.yml`"
    This is a manually triggered workflow for running the Table Loader service to ingest validated data fragments into the database.

    -   **Trigger**: `workflow_dispatch` only. A user must manually start this workflow.
    -   **Purpose**: To provide a safe and audited way to load data into the `qa` or `prod` databases.
    -   **Inputs**:
        -   `environment`: The target environment (`qa` or `prod`).
        -   `batch_id`: The ID of the fragment batch to load.
        -   `dry_run`: If `true`, runs the loader in preview mode.

    !!! info "Key Steps"
        1.  **Validate Inputs**: Ensures the `batch_id` format is correct.
        2.  **SSH Tunnel**: Establishes a secure SSH tunnel to the database for the `qa` or `prod` environment.
        3.  **Verify Connectivity**: Tests the database connection and verifies that the `fragment_resolutions` table exists for audit logging.
        4.  **Verify S3 Batch**: Checks that the specified `batch_id` exists in the correct S3 bucket before proceeding.
        5.  **Run Table Loader**: Executes the `table-loader`'s `main.py` script with the provided `batch_id` and `dry_run` flag.
        6.  **Upload Artifacts**: The logs and any generated reports from the loader are uploaded as artifacts for review.

!!! abstract "Workflow: `redcap-sync.yml`"
    This workflow runs the REDCap Pipeline to sync data from REDCap projects.

    -   **Trigger**:
        -   Scheduled to run daily (`cron: 0 8 * * *`).
        -   Manually via a `workflow_dispatch` event.
    -   **Purpose**: To keep the IDhub database up-to-date with the latest data from connected REDCap projects.

    !!! info "Key Steps"
        1.  **SSH Tunnel**: Establishes a secure SSH tunnel to the database.
        2.  **Run REDCap Pipeline**: Executes the `redcap-pipeline`'s `main.py` script. It can be run for all enabled projects or for a specific project if provided as a manual input.
        3.  **Upload Logs**: The pipeline logs are uploaded as artifacts for auditing and troubleshooting.