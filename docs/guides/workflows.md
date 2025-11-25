# GitHub Actions Workflows Guide

## Overview

This guide provides comprehensive documentation for all GitHub Actions workflows in the IDhub platform, including triggers, inputs, processes, and best practices for CI/CD operations.

## Table of Contents

- [Workflow Overview](#workflow-overview)
- [REDCap Pipeline Workflow](#redcap-pipeline-workflow)
- [Fragment Ingestion Workflow](#fragment-ingestion-workflow)
- [CI/CD Workflows](#cicd-workflows)
- [Deployment Workflows](#deployment-workflows)
- [Monitoring & Alerts](#monitoring--alerts)
- [Workflow Security](#workflow-security)
- [Troubleshooting](#troubleshooting)

---

## Workflow Overview

### Workflow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GitHub Actions Workflows                  │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   REDCap     │  │  Fragment    │  │   CI/CD      │      │
│  │   Pipeline   │  │  Ingestion   │  │   Testing    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Deployment  │  │  Monitoring  │  │   Security   │      │
│  │   (QA/Prod)  │  │   & Alerts   │  │   Scanning   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Workflow Files Location

```
.github/
├── workflows/
│   ├── redcap-pipeline.yml           # REDCap data pipeline
│   ├── fragment-ingestion.yml        # Fragment loading
│   ├── ci-tests.yml                  # Continuous integration
│   ├── deploy-qa.yml                 # QA deployment
│   ├── deploy-production.yml         # Production deployment
│   ├── security-scan.yml             # Security scanning
│   └── monitoring.yml                # Health checks
├── actions/
│   ├── setup-python/                 # Reusable Python setup
│   ├── setup-database/               # Database connection
│   └── notify-slack/                 # Slack notifications
└── scripts/
    ├── setup-ssh-tunnel.sh           # SSH tunnel setup
    ├── validate-batch.sh             # Batch validation
    └── cleanup.sh                    # Cleanup operations
```

---

## REDCap Pipeline Workflow

### Overview

Automated workflow for extracting data from REDCap projects, transforming it, and uploading curated fragments to S3.

### Workflow File

```yaml:.github/workflows/redcap-pipeline.yml
name: REDCap Pipeline

on:
  # Manual trigger
  workflow_dispatch:
    inputs:
      environment:
        description: 'Target environment'
        required: true
        type: choice
        options:
          - qa
          - production
        default: qa

      project:
        description: 'REDCap project (leave empty for all)'
        required: false
        type: string

      dry_run:
        description: 'Dry run mode (no S3 upload)'
        required: false
        type: boolean
        default: false

      batch_size:
        description: 'Batch size for processing'
        required: false
        type: number
        default: 50

  # Scheduled triggers
  schedule:
    # Run daily at 2 AM UTC for production
    - cron: '0 2 * * *'
    # Run every 6 hours for QA
    - cron: '0 */6 * * *'

env:
  PYTHON_VERSION: '3.11'
  AWS_REGION: us-east-1

jobs:
  determine-environment:
    name: Determine Environment
    runs-on: ubuntu-latest
    outputs:
      environment: ${{ steps.set-env.outputs.environment }}
      is_scheduled: ${{ steps.set-env.outputs.is_scheduled }}

    steps:
      - name: Set environment
        id: set-env
        run: |
          if [ "${{ github.event_name }}" = "schedule" ]; then
            # Determine environment based on schedule
            if [ "${{ github.event.schedule }}" = "0 2 * * *" ]; then
              echo "environment=production" >> $GITHUB_OUTPUT
            else
              echo "environment=qa" >> $GITHUB_OUTPUT
            fi
            echo "is_scheduled=true" >> $GITHUB_OUTPUT
          else
            echo "environment=${{ inputs.environment }}" >> $GITHUB_OUTPUT
            echo "is_scheduled=false" >> $GITHUB_OUTPUT
          fi

  validate-inputs:
    name: Validate Inputs
    runs-on: ubuntu-latest
    needs: determine-environment

    steps:
      - name: Validate project name
        if: inputs.project != ''
        run: |
          VALID_PROJECTS=("gap" "uc_demarc" "protect" "sparc" "niddk_ibdgc")
          PROJECT="${{ inputs.project }}"

          if [[ ! " ${VALID_PROJECTS[@]} " =~ " ${PROJECT} " ]]; then
            echo "Error: Invalid project name: $PROJECT"
            echo "Valid projects: ${VALID_PROJECTS[*]}"
            exit 1
          fi

          echo "✓ Project name validated: $PROJECT"

      - name: Validate batch size
        if: inputs.batch_size != ''
        run: |
          BATCH_SIZE=${{ inputs.batch_size }}

          if [ $BATCH_SIZE -lt 1 ] || [ $BATCH_SIZE -gt 200 ]; then
            echo "Error: Batch size must be between 1 and 200"
            exit 1
          fi

          echo "✓ Batch size validated: $BATCH_SIZE"

  setup-ssh-tunnel:
    name: Setup SSH Tunnel
    runs-on: ubuntu-latest
    needs: [determine-environment, validate-inputs]

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup SSH key
        env:
          SSH_PRIVATE_KEY: ${{ secrets.SSH_PRIVATE_KEY }}
        run: |
          mkdir -p ~/.ssh
          echo "$SSH_PRIVATE_KEY" > ~/.ssh/id_rsa
          chmod 600 ~/.ssh/id_rsa
          ssh-keyscan -H ${{ secrets.BASTION_HOST }} >> ~/.ssh/known_hosts

      - name: Create SSH tunnel
        run: |
          # Start SSH tunnel in background
          ssh -f -N -L 5432:${{ secrets.DB_HOST }}:5432 \
            ${{ secrets.BASTION_USER }}@${{ secrets.BASTION_HOST }}

          # Wait for tunnel to be ready
          timeout 30 bash -c 'until nc -z localhost 5432; do sleep 1; done'

          echo "✓ SSH tunnel established"

      - name: Test database connection
        env:
          PGPASSWORD: ${{ secrets.DB_PASSWORD }}
        run: |
          psql -h localhost -U ${{ secrets.DB_USER }} -d ${{ secrets.DB_NAME }} \
            -c "SELECT version();"

          echo "✓ Database connection successful"

  run-pipeline:
    name: Run REDCap Pipeline
    runs-on: ubuntu-latest
    needs: setup-ssh-tunnel
    environment: ${{ needs.determine-environment.outputs.environment }}

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          cache: 'pip'

      - name: Install dependencies
        working-directory: ./redcap-pipeline
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          pip install -r requirements-test.txt

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Setup environment variables
        run: |
          cat > .env << EOF
          ENVIRONMENT=${{ needs.determine-environment.outputs.environment }}
          DB_HOST=localhost
          DB_PORT=5432
          DB_NAME=${{ secrets.DB_NAME }}
          DB_USER=${{ secrets.DB_USER }}
          DB_PASSWORD=${{ secrets.DB_PASSWORD }}

          REDCAP_API_URL=${{ secrets.REDCAP_API_URL }}
          REDCAP_API_TOKEN_GAP=${{ secrets.REDCAP_API_TOKEN_GAP }}
          REDCAP_API_TOKEN_UC_DEMARC=${{ secrets.REDCAP_API_TOKEN_UC_DEMARC }}
          REDCAP_API_TOKEN_PROTECT=${{ secrets.REDCAP_API_TOKEN_PROTECT }}

          GSID_SERVICE_URL=${{ secrets.GSID_SERVICE_URL }}
          GSID_API_KEY=${{ secrets.GSID_API_KEY }}

          S3_BUCKET=${{ secrets.S3_BUCKET }}

          LOG_LEVEL=INFO
          EOF

      - name: Run pipeline - All projects
        if: inputs.project == ''
        working-directory: ./redcap-pipeline
        run: |
          python main.py \
            --all \
            --batch-size ${{ inputs.batch_size || 50 }} \
            ${{ inputs.dry_run && '--dry-run' || '' }}

      - name: Run pipeline - Specific project
        if: inputs.project != ''
        working-directory: ./redcap-pipeline
        run: |
          python main.py \
            --project ${{ inputs.project }} \
            --batch-size ${{ inputs.batch_size || 50 }} \
            ${{ inputs.dry_run && '--dry-run' || '' }}

      - name: Generate summary report
        if: always()
        working-directory: ./redcap-pipeline
        run: |
          python scripts/generate_summary.py \
            --log-file logs/pipeline.log \
            --output-file summary.md

      - name: Upload logs
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: pipeline-logs-${{ github.run_number }}
          path: |
            redcap-pipeline/logs/
            redcap-pipeline/summary.md
          retention-days: 30

      - name: Post summary to PR
        if: github.event_name == 'pull_request'
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const summary = fs.readFileSync('redcap-pipeline/summary.md', 'utf8');

            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: summary
            });

  cleanup:
    name: Cleanup
    runs-on: ubuntu-latest
    needs: [setup-ssh-tunnel, run-pipeline]
    if: always()

    steps:
      - name: Close SSH tunnel
        run: |
          # Kill SSH tunnel processes
          pkill -f "ssh.*5432:.*:5432" || true
          echo "✓ SSH tunnel closed"

      - name: Cleanup temporary files
        run: |
          rm -f ~/.ssh/id_rsa
          rm -f .env
          echo "✓ Temporary files cleaned up"

  notify:
    name: Send Notifications
    runs-on: ubuntu-latest
    needs: [determine-environment, run-pipeline]
    if: always()

    steps:
      - name: Notify Slack - Success
        if: needs.run-pipeline.result == 'success'
        uses: slackapi/slack-github-action@v1
        with:
          webhook-url: ${{ secrets.SLACK_WEBHOOK_URL }}
          payload: |
            {
              "text": "✅ REDCap Pipeline Completed Successfully",
              "blocks": [
                {
                  "type": "section",
                  "text": {
                    "type": "mrkdwn",
                    "text": "*REDCap Pipeline - Success*\n\n*Environment:* ${{ needs.determine-environment.outputs.environment }}\n*Project:* ${{ inputs.project || 'All projects' }}\n*Run:* <${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}|#${{ github.run_number }}>"
                  }
                }
              ]
            }

      - name: Notify Slack - Failure
        if: needs.run-pipeline.result == 'failure'
        uses: slackapi/slack-github-action@v1
        with:
          webhook-url: ${{ secrets.SLACK_WEBHOOK_URL }}
          payload: |
            {
              "text": "❌ REDCap Pipeline Failed",
              "blocks": [
                {
                  "type": "section",
                  "text": {
                    "type": "mrkdwn",
                    "text": "*REDCap Pipeline - Failed*\n\n*Environment:* ${{ needs.determine-environment.outputs.environment }}\n*Project:* ${{ inputs.project || 'All projects' }}\n*Run:* <${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}|#${{ github.run_number }}>\n\n<!channel> Pipeline requires attention"
                  }
                }
              ]
            }
```

### Usage Examples

#### Manual Trigger - Single Project

```bash
# Via GitHub CLI
gh workflow run redcap-pipeline.yml \
  -f environment=qa \
  -f project=gap \
  -f batch_size=50

# Via GitHub UI
# 1. Go to Actions tab
# 2. Select "REDCap Pipeline"
# 3. Click "Run workflow"
# 4. Fill in parameters
```

#### Manual Trigger - All Projects

```bash
gh workflow run redcap-pipeline.yml \
  -f environment=production \
  -f batch_size=100
```

#### Dry Run Mode

```bash
gh workflow run redcap-pipeline.yml \
  -f environment=qa \
  -f project=gap \
  -f dry_run=true
```

---

## Fragment Ingestion Workflow

### Overview

Manual workflow for loading validated fragments from S3 into the database with comprehensive safety checks.

### Workflow File

```yaml:.github/workflows/fragment-ingestion.yml
name: Fragment Ingestion

on:
  workflow_dispatch:
    inputs:
      environment:
        description: 'Target environment'
        required: true
        type: choice
        options:
          - qa
          - production
        default: qa

      batch_id:
        description: 'Batch ID (format: batch_YYYYMMDD_HHMMSS)'
        required: true
        type: string

      dry_run:
        description: 'Dry run mode (preview only, no database changes)'
        required: false
        type: boolean
        default: true

      table_filter:
        description: 'Specific table to load (leave empty for all)'
        required: false
        type: string

env:
  PYTHON_VERSION: '3.11'
  AWS_REGION: us-east-1

jobs:
  validate-batch:
    name: Validate Batch
    runs-on: ubuntu-latest
    outputs:
      batch_valid: ${{ steps.validate.outputs.valid }}
      fragment_count: ${{ steps.validate.outputs.count }}
      tables: ${{ steps.validate.outputs.tables }}

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Validate batch ID format
        id: validate-format
        run: |
          BATCH_ID="${{ inputs.batch_id }}"

          # Validate format: batch_YYYYMMDD_HHMMSS
          if [[ ! $BATCH_ID =~ ^batch_[0-9]{8}_[0-9]{6}$ ]]; then
            echo "Error: Invalid batch ID format"
            echo "Expected: batch_YYYYMMDD_HHMMSS"
            echo "Got: $BATCH_ID"
            exit 1
          fi

          echo "✓ Batch ID format valid"

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Check batch exists in S3
        id: validate
        run: |
          BUCKET="${{ secrets.S3_BUCKET }}"
          BATCH_ID="${{ inputs.batch_id }}"
          PREFIX="validated/$BATCH_ID/"

          # Check if batch exists
          if ! aws s3 ls "s3://$BUCKET/$PREFIX" > /dev/null 2>&1; then
            echo "Error: Batch not found in S3"
            echo "Bucket: $BUCKET"
            echo "Prefix: $PREFIX"
            exit 1
          fi

          # Count fragments
          FRAGMENT_COUNT=$(aws s3 ls "s3://$BUCKET/$PREFIX" --recursive | wc -l)

          if [ $FRAGMENT_COUNT -eq 0 ]; then
            echo "Error: No fragments found in batch"
            exit 1
          fi

          # List tables
          TABLES=$(aws s3 ls "s3://$BUCKET/$PREFIX" | awk '{print $2}' | sed 's/\///' | sort -u | jq -R -s -c 'split("\n")[:-1]')

          echo "valid=true" >> $GITHUB_OUTPUT
          echo "count=$FRAGMENT_COUNT" >> $GITHUB_OUTPUT
          echo "tables=$TABLES" >> $GITHUB_OUTPUT

          echo "✓ Batch validated"
          echo "  - Fragment count: $FRAGMENT_COUNT"
          echo "  - Tables: $TABLES"

      - name: Generate batch summary
        run: |
          cat > batch-summary.md << EOF
          # Batch Validation Summary

          **Batch ID:** \`${{ inputs.batch_id }}\`
          **Environment:** ${{ inputs.environment }}
          **Fragment Count:** ${{ steps.validate.outputs.count }}
          **Tables:** ${{ steps.validate.outputs.tables }}
          **Dry Run:** ${{ inputs.dry_run }}

          ## S3 Location
          \`\`\`
          s3://${{ secrets.S3_BUCKET }}/validated/${{ inputs.batch_id }}/
          \`\`\`

          ## Next Steps
          - Review fragment contents
          - Verify table mappings
          - Confirm load parameters
          EOF

          cat batch-summary.md

      - name: Upload summary
        uses: actions/upload-artifact@v4
        with:
          name: batch-summary
          path: batch-summary.md

  approval-gate:
    name: Approval Required
    runs-on: ubuntu-latest
    needs: validate-batch
    if: inputs.dry_run == false && inputs.environment == 'production'
    environment:
      name: production-approval
      url: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}

    steps:
      - name: Wait for approval
        run: |
          echo "⏳ Waiting for manual approval..."
          echo "This is a PRODUCTION load operation"
          echo "Batch: ${{ inputs.batch_id }}"
          echo "Fragments: ${{ needs.validate-batch.outputs.fragment_count }}"

  setup-database:
    name: Setup Database Connection
    runs-on: ubuntu-latest
    needs: [validate-batch, approval-gate]
    if: always() && needs.validate-batch.result == 'success' && (needs.approval-gate.result == 'success' || needs.approval-gate.result == 'skipped')

    steps:
      - name: Setup SSH tunnel
        env:
          SSH_PRIVATE_KEY: ${{ secrets.SSH_PRIVATE_KEY }}
        run: |
          mkdir -p ~/.ssh
          echo "$SSH_PRIVATE_KEY" > ~/.ssh/id_rsa
          chmod 600 ~/.ssh/id_rsa
          ssh-keyscan -H ${{ secrets.BASTION_HOST }} >> ~/.ssh/known_hosts

          # Start SSH tunnel
          ssh -f -N -L 5432:${{ secrets.DB_HOST }}:5432 \
            ${{ secrets.BASTION_USER }}@${{ secrets.BASTION_HOST }}

          # Wait for tunnel
          timeout 30 bash -c 'until nc -z localhost 5432; do sleep 1; done'

          echo "✓ SSH tunnel established"

      - name: Test connection
        env:
          PGPASSWORD: ${{ secrets.DB_PASSWORD }}
        run: |
          psql -h localhost -U ${{ secrets.DB_USER }} -d ${{ secrets.DB_NAME }} \
            -c "SELECT current_database(), current_user, version();"

  load-fragments:
    name: Load Fragments
    runs-on: ubuntu-latest
    needs: [validate-batch, setup-database]
    environment: ${{ inputs.environment }}

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
          cache: 'pip'

      - name: Install dependencies
        working-directory: ./table-loader
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Setup environment
        run: |
          cat > .env << EOF
          ENVIRONMENT=${{ inputs.environment }}
          DB_HOST=localhost
          DB_PORT=5432
          DB_NAME=${{ secrets.DB_NAME }}
          DB_USER=${{ secrets.DB_USER }}
          DB_PASSWORD=${{ secrets.DB_PASSWORD }}

          S3_BUCKET=${{ secrets.S3_BUCKET }}

          LOG_LEVEL=INFO
          EOF

      - name: Run table loader - Dry run
        if: inputs.dry_run == true
        working-directory: ./table-loader
        run: |
          python main.py \
            --batch-id "${{ inputs.batch_id }}" \
            --dry-run \
            ${{ inputs.table_filter && format('--table {0}', inputs.table_filter) || '' }}

      - name: Run table loader - Live load
        if: inputs.dry_run == false
        working-directory: ./table-loader
        run: |
          python main.py \
            --batch-id "${{ inputs.batch_id }}" \
            ${{ inputs.table_filter && format('--table {0}', inputs.table_filter) || '' }}

      - name: Generate load summary
        if: always()
        working-directory: ./table-loader
        run: |
          python scripts/generate_load_summary.py \
            --batch-id "${{ inputs.batch_id }}" \
            --log-file logs/loader.log \
            --output-file load-summary.md

      - name: Upload logs and summary
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: load-results-${{ github.run_number }}
          path: |
            table-loader/logs/
            table-loader/load-summary.md
          retention-days: 90

      - name: Update batch status
        if: success() && inputs.dry_run == false
        run: |
          # Mark batch as loaded in S3
          aws s3 cp - "s3://${{ secrets.S3_BUCKET }}/validated/${{ inputs.batch_id }}/.loaded" << EOF
          {
            "batch_id": "${{ inputs.batch_id }}",
            "loaded_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
            "loaded_by": "${{ github.actor }}",
            "workflow_run": "${{ github.run_id }}",
            "environment": "${{ inputs.environment }}"
          }
          EOF

  verify-load:
    name: Verify Load
    runs-on: ubuntu-latest
    needs: load-fragments
    if: inputs.dry_run == false

    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Run verification queries
        env:
          PGPASSWORD: ${{ secrets.DB_PASSWORD }}
        run: |
          # Count records loaded
          psql -h localhost -U ${{ secrets.DB_USER }} -d ${{ secrets.DB_NAME }} << EOF

          -- Verify record counts
          SELECT
            'subjects' as table_name,
            COUNT(*) as record_count
          FROM subjects
          WHERE updated_at > NOW() - INTERVAL '1 hour'

          UNION ALL

          SELECT
            'blood' as table_name,
            COUNT(*) as record_count
          FROM blood
          WHERE updated_at > NOW() - INTERVAL '1 hour'

          UNION ALL

          SELECT
            'dna' as table_name,
            COUNT(*) as record_count
          FROM dna
          WHERE updated_at > NOW() - INTERVAL '1 hour';

          EOF

      - name: Check for errors
        env:
          PGPASSWORD: ${{ secrets.DB_PASSWORD }}
        run: |
          # Check validation_queue for errors
          ERROR_COUNT=$(psql -h localhost -U ${{ secrets.DB_USER }} -d ${{ secrets.DB_NAME }} -t -c \
            "SELECT COUNT(*) FROM validation_queue WHERE batch_id = '${{ inputs.batch_id }}' AND status = 'error';")

          if [ $ERROR_COUNT -gt 0 ]; then
            echo "Warning: $ERROR_COUNT fragments had errors during load"

            # Get error details
            psql -h localhost -U ${{ secrets.DB_USER }} -d ${{ secrets.DB_NAME }} -c \
              "SELECT table_name, error_message FROM validation_queue WHERE batch_id = '${{ inputs.batch_id }}' AND status = 'error' LIMIT 10;"
          else
            echo "✓ No errors detected"
          fi

  cleanup:
    name: Cleanup
    runs-on: ubuntu-latest
    needs: [setup-database, load-fragments, verify-load]
    if: always()

    steps:
      - name: Close SSH tunnel
        run: |
          pkill -f "ssh.*5432:.*:5432" || true
          echo "✓ SSH tunnel closed"

      - name: Cleanup files
        run: |
          rm -f ~/.ssh/id_rsa
          rm -f .env
          echo "✓ Cleanup complete"

  notify:
    name: Send Notifications
    runs-on: ubuntu-latest
    needs: [validate-batch, load-fragments, verify-load]
    if: always()

    steps:
      - name: Notify Slack - Success
        if: needs.load-fragments.result == 'success'
        uses: slackapi/slack-github-action@v1
        with:
          webhook-url: ${{ secrets.SLACK_WEBHOOK_URL }}
          payload: |
            {
              "text": "✅ Fragment Ingestion Completed",
              "blocks": [
                {
                  "type": "section",
                  "text": {
                    "type": "mrkdwn",
                    "text": "*Fragment Ingestion - Success*\n\n*Batch ID:* `${{ inputs.batch_id }}`\n*Environment:* ${{ inputs.environment }}\n*Fragments:* ${{ needs.validate-batch.outputs.fragment_count }}\n*Dry Run:* ${{ inputs.dry_run }}\n*Run:* <${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}|#${{ github.run_number }}>"
                  }
                }
              ]
            }

      - name: Notify Slack - Failure
        if: needs.load-fragments.result == 'failure'
        uses: slackapi/slack-github-action@v1
        with:
          webhook-url: ${{ secrets.SLACK_WEBHOOK_URL }}
          payload: |
            {
              "text": "❌ Fragment Ingestion Failed",
              "blocks": [
                {
                  "type": "section",
                  "text": {
                    "type": "mrkdwn",
                    "text": "*Fragment Ingestion - Failed*\n\n*Batch ID:* `${{ inputs.batch_id }}`\n*Environment:* ${{ inputs.environment }}\n*Run:* <${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}|#${{ github.run_number }}>\n\n<!channel> Load operation requires attention"
                  }
                }
              ]
            }
```

### Usage Examples

#### Dry Run (Preview)

```bash
gh workflow run fragment-ingestion.yml \
  -f environment=qa \
  -f batch_id=batch_20240115_140000 \
  -f dry_run=true
```

#### Live Load - QA

```bash
gh workflow run fragment-ingestion.yml \
  -f environment=qa \
  -f batch_id=batch_20240115_140000 \
  -f dry_run=false
```

#### Live Load - Production (Requires Approval)

```bash
gh workflow run fragment-ingestion.yml \
  -f environment=production \
  -f batch_id=batch_20240115_140000 \
  -f dry_run=false
```

#### Load Specific Table

```bash
gh workflow run fragment-ingestion.yml \
  -f environment=qa \
  -f batch_id=batch_20240115_140000 \
  -f table_filter=blood \
  -f dry_run=false
```

---

## CI/CD Workflows

### Continuous Integration

```yaml:.github/workflows/ci-tests.yml
name: CI Tests

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]
  workflow_dispatch:

env:
  PYTHON_VERSION: '3.11'

jobs:
  lint:
    name: Lint Code
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version:

```
