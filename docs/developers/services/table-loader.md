# Table Loader Documentation

## Overview

The Table Loader is the final and most critical service in the IDhub data ingestion pipeline. Its primary responsibility is to safely and intelligently load validated data fragments from the staging area into the central PostgreSQL database. It operates on batches of data that have been processed by the Fragment Validator and queued for loading.

The service's core is the **Universal Update Strategy**, which uses natural keys to perform "upsert" operations (inserting new records and updating existing ones) while protecting immutable fields from being overwritten.

## Architecture

```mermaid
graph TD
    subgraph "Input"
        VQ[(Validation Queue<br/>PostgreSQL)]
        S3[(S3 Bucket<br/>Validated Fragments)]
    end

    subgraph "Table Loader Service"
        MAIN[main.py - CLI Entrypoint]
        CONFIG[Configuration Loader]

        subgraph "Processing Engine"
            BATCH[1. Batch Processor]
            GROUP[2. Group by Table]
            FETCH[3. Fetch Fragments from S3]
            UPSERT[4. Upsert Logic Engine]
        end

        subgraph "Database Interaction"
            LOOKUP[Natural Key Lookup]
            INSERT[INSERT Statement]
            UPDATE[UPDATE Statement]
            AUDIT[Update Queue Status]
        end
    end
    
    subgraph "Output"
        DB[(IDhub Database)]
    end

    VQ -- "Read pending batch" --> MAIN
    MAIN --> BATCH
    CONFIG -- "table_configs.json" --> UPSERT
    
    BATCH --> GROUP
    GROUP --> FETCH
    FETCH -- "Download fragment" --> S3
    FETCH --> UPSERT

    UPSERT --> LOOKUP
    LOOKUP -- "Query record" --> DB
    LOOKUP -- "Exists?" --> UPSERT

    UPSERT -- "New" --> INSERT
    UPSERT -- "Exists" --> UPDATE

    INSERT -- "Write" --> DB
    UPDATE -- "Write" --> DB

    UPSERT -- "Success/Fail" --> AUDIT
    AUDIT -- "Update status" --> VQ

    style VQ fill:#2196F3
    style S3 fill:#FF9800
    style DB fill:#4CAF50
```

## Key Features

!!! info "Key Features"
    -   **Batch Processing**: Loads data in discrete, auditable batches for transactional integrity.
    -   **Natural Key Upserts**: Intelligently inserts or updates records based on business-defined natural keys (e.g., `global_subject_id` + `niddk_no`), not database IDs.
    -   **Immutable Field Protection**: Prevents critical, once-set data (like `created_at` or `global_subject_id`) from being modified.
    -   **Transactional Loading**: Processes records within a database transaction. If any record in a batch fails, the entire transaction can be rolled back to prevent partial loads.
    -   **Auditing**: Updates the status of each fragment in the `validation_queue` to `loaded` or `failed`, providing a clear record of what was loaded and when.

## Universal Update Strategy

!!! abstract "Universal Update Strategy"
    The power of the Table Loader comes from its update strategy, which is configured on a per-table basis.

    1.  **Natural Key Lookup**: For each incoming record, the loader uses the `natural_key` defined in `table_configs.json` to check if a corresponding record already exists in the target database table.
    2.  **Decision Point**:
        -   **If Not Found**: The loader performs an `INSERT` operation to create a new record.
        -   **If Found**: The loader proceeds to the update path.
    3.  **Immutable Field Check**: Before updating, the loader compares the values of `immutable_fields` in the incoming record with the existing database record. If any immutable field has changed, the update is **rejected**, and the fragment is marked as failed.
    4.  **Value Comparison**: If immutable fields are unchanged, the loader compares the remaining fields. If there are no changes, the update is skipped to avoid unnecessary database writes.
    5.  **Perform Update**: If there are changes to mutable fields, the loader performs an `UPDATE` operation on the existing record.

    [➡️ See the full Universal Update Strategy documentation for more details.](../architecture/update-strategy.md)

## Configuration

!!! abstract "Table Configuration (`config/table_configs.json`)"
    This central configuration file dictates the loader's behavior for each table.

    ```json
    {
      "lcl": {
        "natural_key": ["global_subject_id", "niddk_no"],
        "immutable_fields": ["created_at", "global_subject_id"],
        "update_strategy": "upsert"
      },
      "subjects": {
        "natural_key": ["global_subject_id"],
        "immutable_fields": ["created_at", "gsid"],
        "update_strategy": "upsert"
      }
    }
    ```

    -   `natural_key`: An array of fields that together uniquely identify a record.
    -   `immutable_fields`: An array of fields that cannot be changed after a record is first created.
    -   `update_strategy`: The loading strategy to use (e.g., `upsert`, `insert_only`).

!!! abstract "Environment Variables"
    Configure environment variables for the Table Loader service.

    ```bash
    # Database Connection
    DATABASE_URL=postgresql://user:password@host:port/database

    # AWS S3 for fetching fragments
    AWS_ACCESS_KEY_ID=test
    AWS_SECRET_ACCESS_KEY=test
    AWS_ENDPOINT_URL=http://localhost:4566 # For local development
    S3_BUCKET=idhub-dev-fragments

    # Logging
    LOG_LEVEL=INFO
    ```

## Workflow

!!! abstract "Table Loader Workflow"
    The Table Loader is executed for a specific `batch_id`.

    1.  **Query Queue**: The service queries the `validation_queue` for all pending fragments associated with the given `batch_id`.
    2.  **Group by Table**: To optimize database operations, it groups the list of fragments by their target `table_name`.
    3.  **Process Tables Sequentially**: The loader processes one table at a time.
    4.  **Process Fragments**: For each fragment in the table group:
        a.  **Download from S3**: The JSON fragment data is downloaded from the S3 bucket.
        b.  **Execute Upsert Logic**: The Universal Update Strategy is applied to determine whether to `INSERT`, `UPDATE`, `SKIP`, or `REJECT` the record.
        c.  **Database Operation**: The appropriate SQL command is executed against the database.
    5.  **Update Queue Status**: After processing the fragment, its entry in the `validation_queue` is updated:
        -   On success (`INSERT`, `UPDATE`, `SKIP`), the status is set to `loaded`.
        -   On failure (`REJECT`), the status is set to `failed`, and an `error_message` is recorded.
    6.  **Generate Summary**: Once the entire batch is processed, a summary report is generated detailing how many records were inserted, updated, skipped, or failed.

## Error Handling and Auditing

!!! abstract "Error Handling and Auditing"
    The Table Loader is designed to be robust and auditable.

    -   **Load Failures**: If an individual record fails (e.g., an immutable field violation), only that fragment is marked as `failed`. The rest of the batch continues to load.
    -   **Transactional Integrity**: In non-dry-run mode, operations for a batch can be wrapped in a transaction, ensuring that the database is not left in a partially-loaded state in case of a critical service failure.
    -   **Audit Trail**: The `validation_queue` serves as a permanent audit log. By inspecting the queue, an administrator can see the status of every fragment for every batch, along with any error messages for failed loads.

## Usage

!!! abstract "Command-Line Arguments"
    The Table Loader is run from the command line, typically as part of an automated workflow.

    ```bash
    python main.py --batch-id <batch_id> [--table <table_name>] [--dry-run]
    ```

    -   `--batch-id`: **Required**. The unique identifier for the batch to be loaded.
    -   `--table`: (Optional) If specified, only loads fragments for the given table within the batch.
    -   `--dry-run`: (Optional) Executes the entire loading process—including database lookups and value comparisons—but **does not write any data** or update the queue. This is essential for previewing changes and debugging.

!!! tip "Examples"
    Examples of how to use the Table Loader from the command line.

    ```bash
    # Preview the load for a specific batch without making changes
    python main.py --batch-id batch_20251205_103000 --dry-run

    # Perform a live load of the entire batch
    python main.py --batch-id batch_20251205_103000

    # Load only the 'lcl' table from the batch
    python main.py --batch-id batch_20251205_103000 --table lcl
    ```

## Testing

!!! tip "Running Tests"
    The service includes unit and integration tests to validate its core logic.

    ```bash
    # Navigate to the service directory
    cd table-loader

    # Install test dependencies
    pip install -r requirements-test.txt

    # Run all tests
    pytest

    # Run tests with coverage report
    pytest --cov=. --cov-report=html
    ```

## Related Documentation

-   [Universal Update Strategy](../architecture/update-strategy.md)
-   [Data Flow](../architecture/data-flow.md)
-   [Fragment Validator Service](./fragment-validator.md)