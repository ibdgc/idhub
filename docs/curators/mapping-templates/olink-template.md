# Olink Data Template

This template provides an example of how to structure your CSV file for submitting Olink proteomics data to the `fragment-validator`.

### CSV Data Example

```csv
subject_id,identifier_type,center_name,sample_id
IDG-001-A,consortium_id,MSSM,OLINK-001
IDG-002-B,consortium_id,Cedars-Sinai,OLINK-002
LOCAL-999,local_id,Emory,OLINK-999
```

---

### Column Annotations

Below is a description of each column in the template and its purpose.

!!! note "Using Static Fields"
    For fields that have the same value across all records in your file (e.g., a project name that's consistent for the entire batch), you can define them as `static_fields` directly in your [mapping configuration](../creating-mapping-files.md) instead of including them as columns in your CSV. This simplifies your input file.

#### Subject Identification

*   `subject_id`
    *   **Purpose**: Contains the value of the subject's identifier. This is the primary column used to find the subject in the database.
*   `identifier_type`
    *   **Purpose**: Specifies what *type* of ID is in the `subject_id` column for that row (e.g., `consortium_id`, `local_id`).


#### Center Identification

*   `center_name`
    *   **Purpose**: The name of the center where the data originated. This name is used to look up the correct center ID.

#### Olink Data Fields

These columns map directly to the fields in the `olink` table in ID Hub.

*   `sample_id`
    *   **Purpose**: The unique identifier for this specific Olink sample.

---

### Mapping Configuration Example

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
