# Manual Data Ingestion Guide

This guide provides a step-by-step process for curators to manually prepare, validate, and load data into IDhub. This workflow is used for data that does not come from an automated source like the REDCap pipeline.

## Local Environment Setup

Before you can run the validator script on your local machine, you need to set up your environment with the correct tools and credentials.

!!! abstract "Step 1: Conda Environment Setup"
    The project uses Conda to manage Python and its dependencies, ensuring everyone runs the same version of the tools.

    1.  **Install Conda**: If you don't have it, install [Miniconda](https://docs.conda.io/en/latest/miniconda.html) or Anaconda.
    2.  **Create the Environment**: From the root directory of the `idhub` project, run the following command. This will create a new environment named `idhub-dev` using the project's `environment.yml` file.
        ```bash
        conda env create -f environment.yml
        ```
    3.  **Activate the Environment**: Before running any scripts, you must activate the environment each time you open a new terminal:
        ```bash
        conda activate idhub-dev
        ```
        Your terminal prompt should now show `(idhub-dev)` at the beginning.

!!! abstract "Step 2: Obtain and Use API Keys"
    You will need two keys to run the validator. It is critical to use the correct key for the environment (`qa` or `production`) you are targeting.

    - **`NOCODB_TOKEN`**: This key allows the validator to connect to the NocoDB API.
    - **`GSID_API_KEY`**: This key allows the validator to communicate with the subject identity service.

    === "For the QA Environment"
        - **`NOCODB_TOKEN`**: Generate this key yourself from the NocoDB web interface. Log into `qa.idhub.ibdgc.org`, click the gear settings icon in the left navigation, and go to the "Tokens" section.
        - **`GSID_API_KEY`**: This key is managed by the system administrators. Please contact them to obtain the **QA** key.

    === "For the Production Environment"
        - **`NOCODB_TOKEN`**: Generate this key yourself from the NocoDB web interface. Log into `idhub.ibdgc.org`, click the gear settings icon in the left navigation, and go to the "Tokens" section.
        - **`GSID_API_KEY`**: This key is managed by the system administrators. Please contact them to obtain the **Production** key.


!!! abstract "Step 3: Configure AWS Credentials"
    The `fragment-validator` interacts with AWS S3 staging buckets. To authenticate these interactions in your local environment, you need to configure your AWS credentials. The validator will automatically pick up credentials configured for the AWS Command Line Interface (CLI).

    1.  **Install AWS CLI**: If you don't have it, install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
    2.  **Configure AWS CLI**: Open your terminal and run the following command. You will be prompted to enter your `AWS Access Key ID`, `AWS Secret Access Key`, and your preferred default region.
        ```bash
        aws configure
        ```
    3.  **Verify Configuration**: You can verify your setup by trying to list S3 buckets (ensure you have permissions for this):
        ```bash
        aws s3 ls
        ```
        This step is crucial for the fragment validator to be able to read and write to the S3 staging buckets.

!!! abstract "Step 4: Create a Secure `.env` File"
    The validator requires secret API keys to communicate with other IDhub services. These are managed in a local `.env` file, which is a plain text file you create in the root of the `idhub` project directory.

    !!! warning "DO NOT COMMIT OR SHARE THIS FILE"
        The `.env` file contains sensitive credentials and is listed in `.gitignore` to prevent it from ever being saved to the git repository. **Never** share this file or commit it.

    1.  **Create the file**: In the root of the `idhub` project, create a new file named `.env`.
    2.  **Restrict .env permissions:** The `.env` file contains highly sensitive keys, so it should only be accessible by your user.
        ```bash
        # change the permissions using chmod
        chmod 600 .env
        ```
    3.  **Add content**: Copy and paste the following template into your `.env` file and add the keys you obtained.

        ```bash
        # .env file for local fragment validation

        # For QA, use the QA token. For Production, use the Production token.
        NOCODB_TOKEN="your_nocodb_token"

        # For QA, use the QA key. For Production, use the Production key.
        GSID_API_KEY="your_gsid_api_key"
        ```
        The script will automatically load them when you run it.

---

## Workflow Execution

The fragment ingestion process involves two main services, which you can trigger via the GitHub Actions interface:

1.  **Fragment Validator**: Validates your data file and converts it into standardized "fragments".
2.  **Table Loader**: Loads the validated fragments into the database.

!!! tip "Step 1: Prepare Your Data File"
    Before you can ingest data, you must prepare your file according to IDhub's requirements.

    - **Format**: Use CSV
    - **Header**: The first row of your file **must** be a header row with clear column names.
    - **Content**: Ensure your file contains all required fields for the table you are loading, especially a **subject identifier** (like `consortium_id`) and a **unique identifier** for the record (like `sample_id`). There can be multiple candidate subject IDs.

    [See more on data preparation →](ingestion-summary.md)

!!! tip "Step 2: Configure Table Mappings"
    The validation process uses configuration files to understand how to process your data. Specifically, `mapping.json` files tell the validator how columns in your file map to fields in the database.

    - **When to create a new map?**: You only need to worry about this if you are submitting a **new type of file** with a different structure or new columns that the system has not seen before.
    - **What to do**: If you have a file with a new structure, you must follow the mapping file creation guide to generate or update a `mapping.json` file.

    [See more on creating mapping files →](creating-mapping-files.md)

    !!! note "Existing Table Mappings"
    	For existing, known file formats, these configurations will already be in place in the `fragment-validator/config` directory.

!!! tip "Step 3: Validate the Data with the Fragment Validator"
    Once your file is ready, you will use the **Fragment Validator** to check your data and create validated fragments. If you are running the environment locally, you can run the validator from the command line:

    ```bash
    # Navigate to the fragment-validator directory
    cd idhub/fragment-validator/

    # Run the validator
    python main.py \
      --table-name lcl \
      --input-file /path/to/your/data.csv \
      --mapping-config config/lcl_mapping.json \
      --source "name_of_source_file.csv" \
      --env production
    ```
    
    1.  **`--table-name`**: The target IDhub table destination (e.g., `lcl`, `genotype`).
    2.  **`--input-file`**: The path to your local data file.
    3.  **`--mapping-config`**: The path to the JSON mapping file that describes your data.
    4.  **`--source`**: A string describing the source of the data for auditing purposes.
    5.  **`--env`**: The target environment (`production` or `qa`). Defaults to `qa`.

!!! tip "Step 4: Load the Validated Fragments"
    After the Fragment Validator runs successfully and generates a **Batch ID**, you can use the **Table Loader** to load this batch into the database.

    === "Using GitHub Actions GUI (Recommended)"
        1.  Go to the **`Actions`** tab in the IDhub GitHub repository.
        2.  Find the **"Fragment Ingestion Pipeline"** workflow in the list on the left.
        3.  Click the **"Run workflow"** button on the right side of the navigation.
        4.  **Fill out the form**:
            - **`environment`**: Choose the **same environment** you used for validation (`qa` or `production`).
            - **`batch_id`**: **Paste the Batch ID** you copied from the successful Fragment Validator run.
            - **`dry_run`**: true / false
        5.  **Run the workflow**. Consider running with `dry_run` checked initially. Review the output log to ensure the changes are what you expect. If everything looks correct, run the workflow again with `dry_run` unchecked to perform the live load.

    === "Using the CLI"
        You can also run the Table Loader from the command line for local development. This requires the appropriate environment variable configuration.

        ```bash
        # Navigate to the table-loader directory
        cd table-loader/

        # First, run in dry-run mode to preview changes
        python main.py --batch-id <your_batch_id> --approve False

        # If the dry run looks correct, run the live load
        python main.py --batch-id <your_batch_id> --approve True
        ```
