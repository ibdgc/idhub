# Creating a Validator Mapping File

The JSON mapping file is the most important piece of configuration for the Fragment Validator. It acts as a "Rosetta Stone," telling the validator how to interpret your source CSV file and transform it into a standardized format that IDhub can understand.

This guide breaks down each section of the mapping file with explanations and examples.

---

### Full Example Template

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

---

### Section-by-Section Explanation

#### `field_mapping`
*   **Purpose**: To map columns from your source CSV file to their target columns in the database.
*   **Format**: A dictionary where the `"key"` is the **target database column name** and the `"value"` is the **header name of the source column** in your CSV.
*   **Example**:
    ```json
    "field_mapping": {
      "sample_id": "collaborator_sample_id"
    }
    ```
    This tells the validator: "For the database's `sample_id` field, get the data from my CSV's `collaborator_sample_id` column."

#### `static_fields`
*   **Purpose**: To assign a fixed, constant value to a database field for **every row** in your file. This is useful when a value is the same for all records in a batch (e.g., the project name or sample type).
*   **Format**: A dictionary where the `"key"` is the **target database column name** and the `"value"` is the **static value** you want to assign.
*   **Example**:
    ```json
    "static_fields": {
      "project": "IBDGC-PRO",
      "sample_type": "bge"
    }
    ```
    This will set the `project` field to "IBDGC-PRO" and the `sample_type` field to "bge" for all records processed with this mapping.

#### `subject_id_candidates`
*   **Purpose**: To tell the validator which column(s) to use to identify the subject for each row. The validator will check these in order. This is the most flexible and powerful feature for subject resolution.
*   **Format**: A dictionary where the `"key"` is the **header name of the source column** in your CSV, and the `"value"` is the **`identifier_type`** that corresponds to that ID.
*   **Example**:
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
*   **Purpose**: An alternative way to specify the `identifier_type`. If this field is set, the validator will look for a column in your CSV with this name and use its value as the `identifier_type` for all candidates.
*   **Format**: A string containing a column name from your CSV.
*   **Example**:
    ```json
    "subject_id_type_field": "type_of_id"
    ```
    If your CSV has a `type_of_id` column, the validator will use the value in that column for each row (e.g., "consortium_id" or "local_id"). This is generally less flexible than the dictionary format for `subject_id_candidates` and should only be used in specific cases. Set it to `null` if you are using the dictionary format.

#### `center_id_field`
*   **Purpose**: To specify which column in your CSV contains the **name** of the center associated with the record.
*   **Format**: A string containing a column name.
*   **Example**:
    ```json
    "center_id_field": "center_name"
    ```
    The validator will take the value from this column (e.g., "MSSM", "Cedars-Sinai") and use its fuzzy-matching and alias logic to find the correct numeric center ID.

#### `default_center_id`
*   **Purpose**: A fallback numeric ID to use if the `center_id_field` is not provided in the mapping, or if the column is empty for a given row.
*   **Format**: An integer.

#### `exclude_from_load`
*   **Purpose**: To list any columns from your source CSV that are needed for validation (like `consortium_id`) but should **not** be loaded into the final data table itself. This prevents metadata used for mapping from being incorrectly inserted as data.
*   **Format**: A list of strings.
*   **Example**:
    ```json
    "exclude_from_load": ["consortium_id", "center_id"]
    ```
