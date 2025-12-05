# Manual Data Ingestion Guide

This guide provides a step-by-step process for curators to manually prepare, validate, and load data into IDhub. This workflow is used for data that does not come from an automated source like the REDCap pipeline.

The process involves two main services, which you can trigger via the GitHub Actions interface:

1.  **Fragment Validator**: Validates your data file and converts it into standardized "fragments".
2.  **Table Loader**: Loads the validated fragments into the database.

## Step 1: Prepare Your Data File

Before you can ingest data, you must prepare your file according to IDhub's requirements.

- **Format**: Use CSV
- **Header**: The first row of your file **must** be a header row with clear column names.
- **Content**: Ensure your file contains all required fields for the table you are loading, especially a **subject identifier** (like `consortium_id`) and a unique identifier for the record (like `sample_id`). There can be multiple candidate subject IDs.

### minimal required fields

### rubric field constraints

[See more on data preparation →](ingestion-overview.md)

## Step 2: Configure Table Mappings

The validation process uses configuration files to understand how to process your data. Specifically, `mapping.json` files tell the validator how columns in your file map to fields in the database.

- **When is this needed?**: You only need to worry about this if you are submitting a **new type of file** with a different structure or new columns that the system has not seen before.
- **What to do**: If you have a file with a new structure, you must work with the developer team to create or update a `mapping.json` file.
- **Example Mapping**: This configuration tells the system that the `source_id` column in your CSV should be mapped to the `sample_id` field in the database, and that the `subject_identifier` column should be used to find the subject's GSID.

```json
{
  "field_mapping": {                        # content fields
    "sample_id": "collaborator_sample_id",
    "sample_type": "sample_type",
    "batch": "batch",
    "identifier_type": "identifier_type"    # this is the type of subject ID e.g. consortium_id
  },
  "subject_id_candidates": ["subject_id"],  # can include multiple
  "subject_id_type_field": "identifier_type",
  "center_id_field": "center_id",
  "default_center_id": 1,
  "exclude_from_load": ["subject_id", "center_id", "identifier_type"]   # excluded from sample table
}
```

For existing, known file formats, these configurations will already be in place.

## Step 3: Validate the Data with the Fragment Validator

Once your file is ready, you will use the **Fragment Validator** to check your data and create validated fragments. If you are running the environment locally, you can run the validator from the command line:

```bash
# Navigate to the fragment-validator directory
cd idhub/fragment-validator/

# Run the validator
python main.py \
  --table-name lcl \
  --input-file /path/to/your/data.csv \
  --mapping-config config/lcl_mapping.json \
  --source "name_of_source_file.csv"
  --env production        # defaults to qa
```

---

## Step 4: Load the Validated Fragments

After the Fragment Validator runs successfully and generates a **Batch ID**, you can use the **Table Loader** to load this batch into the database.

### Using the GitHub Actions Interface (Recommended)

1.  Go to the **`Actions`** tab in the IDhub GitHub repository.
2.  Find the **"Fragment Ingestion Pipeline"** workflow in the list on the left.
3.  Click the **"Run workflow"** button on the right side of the navigation.
4.  **Fill out the form**:
    - **`environment`**: Choose the **same environment** you used for validation (`qa` or `production`).
    - **`batch_id`**: **Paste the Batch ID** you copied from the successful Fragment Validator run.
    - **`dry_run`**: true / false
5.  **Run the workflow**. Consider running with `dry_run` checked initially. Review the output log to ensure the changes are what you expect. If everything looks correct, run the workflow again with `dry_run` unchecked to perform the live load.

### Using the CLI (For Developers/Advanced Users)

You can also run the Table Loader from the command line for local development.

```bash
# Navigate to the table-loader directory
cd table-loader/

# First, run in dry-run mode to preview changes
python main.py --batch-id <your_batch_id> --approve False

# If the dry run looks correct, run the live load
python main.py --batch-id <your_batch_id> --approve True
```
