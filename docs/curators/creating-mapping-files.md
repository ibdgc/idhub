# Creating a Validator Mapping File

The JSON mapping file is the most important piece of configuration for the Fragment Validator. It acts as a "Rosetta Stone," telling the validator how to interpret your source CSV file and transform it into a standardized format that IDhub can understand.

This guide breaks down each section of the mapping file with explanations and examples.

---

### Section-by-Section Explanation

#### `field_mapping`

- **Purpose**: To map columns from your source CSV file to their target columns in the database.
- **Format**: A dictionary where the `"key"` is the **target database column name** and the `"value"` is the **header name of the source column** in your CSV.
- **Example**:
  ```json
  "field_mapping": {
    "sample_id": "collaborator_sample_id"
  }
  ```
  This tells the validator: "For the database's `sample_id` field, get the data from my CSV's `collaborator_sample_id` column."

#### `static_fields`

- **Purpose**: To assign a fixed, constant value to a database field for **every row** in your file. This is useful when a value is the same for all records in a batch (e.g., the project name or sample type).
- **Format**: A dictionary where the `"key"` is the **target database column name** and the `"value"` is the **static value** you want to assign.
- **Example**:
  ```json
  "static_fields": {
    "project": "IBDGC-PRO",
    "sample_type": "bge"
  }
  ```
  This will set the `project` field to "IBDGC-PRO" and the `sample_type` field to "bge" for all records processed with this mapping.

#### `subject_id_candidates`

- **Purpose**: To tell the validator which column(s) to use to identify the subject for each row. The validator will check these in order. This is the most flexible and powerful feature for subject resolution.
- **Format**: A dictionary where the `"key"` is the **header name of the source column** in your CSV, and the `"value"` is the **`identifier_type`** that corresponds to that ID.
- **Example**:

  ```json
  "subject_id_candidates": {
    "consortium_id": "consortium_id",
    "niddk_no": "local_id"
  }
  ```

  This tells the validator: "For each row, first look in the `consortium_id` column. If you find a value, treat it as a `consortium_id`. If that column is empty, look in the `niddk_no` column and treat that value as a `local_id`."

  !!! note "Backward Compatibility"
  The validator also supports an older format where this field is a simple list of strings (e.g., `["consortium_id", "subject_id"]`). In that case, the validator will use the column name itself as the `identifier_type`. The dictionary format is preferred for clarity and flexibility.

#### `subject_id_type_field`

- **Purpose**: An alternative way to specify the `identifier_type`. If this field is set, the validator will look for a column in your CSV with this name and use its value as the `identifier_type` for all candidates.
- **Format**: A string containing a column name from your CSV.
- **Example**:
  ```json
  "subject_id_type_field": "type_of_id"
  ```
  If your CSV has a `type_of_id` column, the validator will use the value in that column for each row (e.g., "consortium_id" or "local_id"). This is generally less flexible than the dictionary format for `subject_id_candidates` and should only be used in specific cases. Set it to `null` if you are using the dictionary format.

#### `center_id_field`

- **Purpose**: To specify which column in your CSV contains the **name** of the center associated with the record.
- **Format**: A string containing a column name.
- **Example**:
  ```json
  "center_id_field": "center_name"
  ```
  The validator will take the value from this column (e.g., "MSSM", "Cedars-Sinai") and use its fuzzy-matching and alias logic to find the correct numeric center ID.

#### `default_center_id`

- **Purpose**: A fallback numeric ID to use if the `center_id_field` is not provided in the mapping, or if the column is empty for a given row.
- **Format**: An integer.

#### `exclude_from_load`

- **Purpose**: To list any columns from your source CSV that are needed for validation (like `consortium_id`) but should **not** be loaded into the final data table itself. This prevents metadata used for mapping from being incorrectly inserted as data.
- **Format**: A list of strings.
- **Example**:
  ```json
  "exclude_from_load": ["consortium_id", "center_id"]
  ```

---

### Table-Specific Templates

=== "Full Template"

    Here is a complete example of a mapping file that uses all available features.

    ```json
    {
      "field_mapping": {
        "sample_id": "collaborator_sample_id",
        "knumber": "k_number"
      },
      "static_fields": {
        "project": "IBDGC-MAIN",
        "sample_type": "bge"
      },
      "subject_id_candidates": {
        "consortium_id": "consortium_id",
        "niddk_no": "local_id"
      },
      "subject_id_type_field": null,
      "center_id_field": "center_name",
      "default_center_id": 1,
      "exclude_from_load": ["consortium_id"]
    }
    ```

=== "LCL"

    This template provides an example of how to structure your CSV file for submitting Lymphoblastoid Cell Line (LCL) data to the `fragment-validator`.

    **Mapping Configuration Example**

    This is the JSON mapping configuration (`lcl_mapping_example.json`) that corresponds to this data template.

    ```json
    {
      "field_mapping": {
        "knumber": "knumber",
        "niddk_no": "niddk_no"
      },
      "subject_id_candidates": {
        "consortium_id": "consortium_id",
        "niddk_no": "niddk_no"
      },
      "center_id_field": "center_name",
      "default_center_id": 1,
      "exclude_from_load": ["consortium_id", "center_id", "identifier_type"]
    }
    ```

    ---

    **CSV Data Example**

    ```csv
    consortium_id,niddk_no,knumber,center_name
    IDG-001-A,NIDDK-1111,K1111,MSSM
    IDG-002-B,,K2222,Cedars-Sinai
    ,NIDDK-3333,K3333,Emory
    ```

    ---

    **Column Annotations**

    Below is a description of each column in the template and its purpose.

    **Subject Identification**

    The validator uses one or more columns to find the correct subject in the database. For the LCL table, it will try the following columns in order. At least one of these must have a value for each row.

    *   `subject_id`
        *   **Purpose**: Contains the value of the subject's identifier. This is the primary column used to find the subject in the database.
    *   `consortium_id`
        *   **Purpose**: The primary IBDGC identifier for a subject.
        *   **Identifier Type**: `consortium_id`
    *   `niddk_no`
        *   **Purpose**: An alternative subject identifier (the NIDDK number). This column also contains the data for the `niddk_no` field in the LCL table itself.
        *   **Identifier Type**: `niddk_no`

    **Center Identification**

    - `center_name`
      - **Purpose**: The name of the center where the data originated. This name is used to look up the correct center ID.
      - **Notes**: The system uses fuzzy matching and a list of aliases to find the correct center. For example, "MSSM", "Sinai", and "mount_sinai" will all resolve to the same center.

    **LCL Data Fields**

    These columns map directly to the fields in the `lcl` table in ID Hub.

    - `knumber`
      - **Purpose**: The "K-number" identifier for the cell line.
    - `niddk_no`
      - **Purpose**: The NIDDK number associated with the cell line. Note that this field serves a dual purpose: it's used as a subject ID candidate _and_ as data for the LCL record.

=== "Enteroid"

    This template provides an example of how to structure your CSV file for submitting enteroid data to the `fragment-validator`.

    **Mapping Configuration Example**

    This is the JSON mapping configuration (`enteroid_mapping_example.json`) that corresponds to this data template.

    ```json
    {
      "field_mapping": {
        "sample_id": "sample_id"
      },
      "subject_id_candidates": {
        "subject_id": "consortium_id"
      },
      "subject_id_type_field": "identifier_type",
      "center_id_field": "center_name",
      "default_center_id": 1,
      "exclude_from_load": ["subject_id", "center_id", "identifier_type"]
    }
    ```

    ---

    **CSV Data Example**

    ```csv
    subject_id,identifier_type,center_name,sample_id
    IDG-001-A,consortium_id,MSSM,ENT-001
    IDG-002-B,consortium_id,Cedars-Sinai,ENT-002
    LOCAL-999,local_id,Emory,ENT-999
    ```

    ---

    **Column Annotations**

    Below is a description of each column in the template and its purpose.

    **Subject Identification**

    *   `subject_id`
        *   **Purpose**: Contains the value of the subject's identifier. This is the primary column used to find the subject in the database.
    *   `identifier_type`
        *   **Purpose**: Specifies what *type* of ID is in the `subject_id` column for that row (e.g., `consortium_id`, `local_id`).


    **Center Identification**

    *   `center_name`
        *   **Purpose**: The name of the center where the data originated. This name is used to look up the correct center ID.

    **Enteroid Data Fields**

    These columns map directly to the fields in the `enteroid` table in ID Hub.

    *   `sample_id`
        *   **Purpose**: The unique identifier for this specific enteroid sample.

=== "Genotype"

    This template provides an example of how to structure your CSV file for submitting genotype data to the `fragment-validator`.

    **Mapping Configuration Example**

    This is the JSON mapping configuration (`genotype_mapping_example.json`) that corresponds to this data template.

    ```json
    {
      "field_mapping": {
        "genotype_id": "id",
        "genotyping_project": "project",
        "genotyping_barcode": "barcode"
      },
      "subject_id_candidates": {
        "consortium_id": "consortium_id"
      },
      "subject_id_type_field": "identifier_type",
      "center_id_field": "center_name",
      "default_center_id": 1,
      "exclude_from_load": ["consortium_id", "center_id", "identifier_type"]
    }
    ```

    ---

    **CSV Data Example**

    ```csv
    consortium_id,identifier_type,center_name,id,project,barcode
    IDG-001-A,consortium_id,MSSM,GENO-001,GSA-Array-v1,987654321
    IDG-002-B,consortium_id,Cedars-Sinai,GENO-002,GSA-Array-v1,987654322
    IDG-003-C,consortium_id,Emory,GENO-003,GSA-Array-v2,987654323
    ```

    ---

    **Column Annotations**

    Below is a description of each column in the template and its purpose.

    **Subject Identification**

    *   `consortium_id`
        *   **Purpose**: The primary IBDGC identifier for a subject.
    *   `identifier_type`
        *   **Purpose**: Specifies what *type* of ID is in the `consortium_id` column for that row. For this mapping, it will typically be `consortium_id`.


    **Center Identification**

    *   `center_name`
        *   **Purpose**: The name of the center where the data originated. Although the current `genotype_mapping.json` does not specify a `center_id_field`, providing this column allows for future-proofing and consistency.

    **Genotype Data Fields**

    These columns map directly to the fields in the `genotype` table in ID Hub. The header names here (`id`, `project`, `barcode`) are the expected *source* column names as defined in the `field_mapping`.

    *   `id`
        *   **Purpose**: The unique identifier for this specific genotype record. This will be mapped to the `genotype_id` column in the database.
    *   `project`
        *   **Purpose**: The name of the genotyping project (e.g., `GSA-Array-v1`). This maps to the `genotyping_project` column.
    *   `barcode`
        *   **Purpose**: The barcode of the genotyping array. This maps to the `genotyping_barcode` column.

=== "Olink"

    This template provides an example of how to structure your CSV file for submitting Olink proteomics data to the `fragment-validator`.

    **Mapping Configuration Example**

    This is the JSON mapping configuration (`olink_mapping_example.json`) that corresponds to this data template.

    ```json
    {
      "field_mapping": {
        "sample_id": "sample_id"
      },
      "subject_id_candidates": {
        "subject_id": "consortium_id"
      },
      "subject_id_type_field": "identifier_type",
      "center_id_field": "center_name",
      "default_center_id": 1,
      "exclude_from_load": ["subject_id", "center_id", "identifier_type"]
    }
    ```

    ---

    **CSV Data Example**

    ```csv
    subject_id,identifier_type,center_name,sample_id
    IDG-001-A,consortium_id,MSSM,OLINK-001
    IDG-002-B,consortium_id,Cedars-Sinai,OLINK-002
    LOCAL-999,local_id,Emory,OLINK-999
    ```

    ---

    **Column Annotations**

    Below is a description of each column in the template and its purpose.

    **Subject Identification**

    *   `subject_id`
        *   **Purpose**: Contains the value of the subject's identifier. This is the primary column used to find the subject in the database.
    *   `identifier_type`
        *   **Purpose**: Specifies what *type* of ID is in the `subject_id` column for that row (e.g., `consortium_id`, `local_id`).


    **Center Identification**

    *   `center_name`
        *   **Purpose**: The name of the center where the data originated. This name is used to look up the correct center ID.

    **Olink Data Fields**

    These columns map directly to the fields in the `olink` table in ID Hub.

    *   `sample_id`
        *   **Purpose**: The unique identifier for this specific Olink sample.

=== "Sequence"

    This template provides an example of how to structure your CSV file for submitting sequencing data to the `fragment-validator`.

    **Mapping Configuration Example**

    This is the JSON mapping configuration (`sequence_mapping_example.json`) that corresponds to this data template.

    ```json
    {
      "field_mapping": {
        "sample_id": "sample_id",
        "sample_type": "sample_type",
        "vcf_sample_id": "vcf_sample_id"
      },
      "subject_id_candidates": {
        "consortium_id": "consortium_id"
      },
      "center_id_field": "center_name",
      "default_center_id": 1,
      "exclude_from_load": ["consortium_id", "center_id"]
    }
    ```

    ---

    **CSV Data Example**

    ```csv
    consortium_id,center_name,sample_id,sample_type,vcf_sample_id
    IDG-001-A,MSSM,SEQ-001,WGS,SAM-001A
    IDG-002-B,Cedars-Sinai,SEQ-002,RNA-Seq,SAM-002B
    IDG-003-C,Emory,SEQ-003,16S,SAM-003C
    ```

    ---

    **Column Annotations**

    Below is a description of each column in the template and its purpose.

    **Subject Identification**

    *   `consortium_id`
        *   **Purpose**: The primary IBDGC identifier for a subject. This is used to find the correct subject in the database.


    **Center Identification**

    *   `center_name`
        *   **Purpose**: The name of the center where the data originated. This name is used to look up the correct center ID.

    **Sequence Data Fields**

    These columns map directly to the fields in the `sequence` table in ID Hub. The header names here are examples of common source column names.

    *   `sample_id`
        *   **Purpose**: The unique identifier for this specific sequencing sample.
    *   `sample_type`
        *   **Purpose**: The type of sequencing performed (e.g., `WGS`, `RNA-Seq`, `16S`).
    *   `vcf_sample_id`
        *   **Purpose**: The sample identifier found within the VCF file, if applicable.

=== "Specimen"

    This template provides an example of how to structure your CSV file for submitting general specimen data to the `fragment-validator`.

    **Mapping Configuration Example**

    This is the JSON mapping configuration (`specimen_mapping_example.json`) that corresponds to this data template.

    ```json
    {
      "field_mapping": {
        "sample_id": "sample_id",
        "sample_type": "sample_type",
        "year_collected": "year_collected",
        "redcap_event": "redcap_event",
        "region_location": "region_location",
        "sample_available": "sample_available",
        "project": "project",
        "identifier_type": "identifier_type"
      },
      "subject_id_candidates": {
        "subject_id": "consortium_id"
      },
      "subject_id_type_field": "identifier_type",
      "center_id_field": "center_name",
      "default_center_id": 1,
      "exclude_from_load": ["subject_id", "center_id", "identifier_type"]
    }
    ```

    ---

    **CSV Data Example**

    ```csv
    subject_id,identifier_type,center_name,sample_id,sample_type,year_collected,project
    IDG-001-A,consortium_id,MSSM,SPEC-001A,Plasma,2023,IBDGC-PRO
    IDG-002-B,consortium_id,Cedars-Sinai,SPEC-002B,Serum,2024,IBDGC-PRO
    LOCAL-999,local_id,Emory,SPEC-999Z,Stool,2024,Immuno-Chip
    ```

    ---

    **Column Annotations**

    Below is a description of each column in the template and its purpose.

    **Subject Identification**

    *   `subject_id`
        *   **Purpose**: Contains the value of the subject's identifier. This is the primary column used to find the subject in the database.
        *   **Note**: The header name `subject_id` is defined by the `subject_id_candidates` array in the mapping configuration. If you used a different column name in your source file (e.g., `participant_id`), you would update the `subject_id_candidates` to `["participant_id"]`.

    *   `identifier_type`
        *   **Purpose**: Specifies what *type* of ID is in the `subject_id` column for that row.
        *   **Notes**: This single type is applied to all fields listed in `subject_id_candidates`. Common values are `consortium_id` or `local_id`. The name of this column itself (`identifier_type`) is defined by the `subject_id_type_field` in the mapping configuration.

    **Center Identification**

    *   `center_name`
        *   **Purpose**: The name of the center where the data originated.
        *   **Notes**: The system uses this name to find the correct numeric center ID. The header name `center_name` is defined by the `center_id_field` in the mapping configuration.

    **Specimen Data Fields**

    These columns map directly to the fields in the `specimen` table in ID Hub. The header names here (`sample_id`, `sample_type`, etc.) are the expected *source* column names as defined in the `field_mapping` section of the config.

    *   `sample_id`
        *   **Purpose**: The unique identifier for this specific specimen.
    *   `sample_type`
        *   **Purpose**: The type of specimen (e.g., `Plasma`, `Serum`, `Stool`).
    *   `year_collected`
        *   **Purpose**: The year the specimen was collected.
    *   `project`
        *   **Purpose**: The name of the project associated with this specimen.
