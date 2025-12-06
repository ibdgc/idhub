# Specimen Data Template

This template provides an example of how to structure your CSV file for submitting general specimen data to the `fragment-validator`.

### CSV Data Example

```csv
subject_id,identifier_type,center_name,sample_id,sample_type,year_collected,project
IDG-001-A,consortium_id,MSSM,SPEC-001A,Plasma,2023,IBDGC-PRO
IDG-002-B,consortium_id,Cedars-Sinai,SPEC-002B,Serum,2024,IBDGC-PRO
LOCAL-999,local_id,Emory,SPEC-999Z,Stool,2024,Immuno-Chip
```

---

### Column Annotations

Below is a description of each column in the template and its purpose.

!!! note "Using Static Fields"
    For fields that have the same value across all records in your file (e.g., a project name that's consistent for the entire batch), you can define them as `static_fields` directly in your [mapping configuration](../creating-mapping-files.md) instead of including them as columns in your CSV. This simplifies your input file.

#### Subject Identification

*   `subject_id`
    *   **Purpose**: Contains the value of the subject's identifier. This is the primary column used to find the subject in the database.
    *   **Note**: The header name `subject_id` is defined by the `subject_id_candidates` array in the mapping configuration. If you used a different column name in your source file (e.g., `participant_id`), you would update the `subject_id_candidates` to `["participant_id"]`.

*   `identifier_type`
    *   **Purpose**: Specifies what *type* of ID is in the `subject_id` column for that row.
    *   **Notes**: This single type is applied to all fields listed in `subject_id_candidates`. Common values are `consortium_id` or `local_id`. The name of this column itself (`identifier_type`) is defined by the `subject_id_type_field` in the mapping configuration.

#### Center Identification

*   `center_name`
    *   **Purpose**: The name of the center where the data originated.
    *   **Notes**: The system uses this name to find the correct numeric center ID. The header name `center_name` is defined by the `center_id_field` in the mapping configuration.

#### Specimen Data Fields

These columns map directly to the fields in the `specimen` table in ID Hub. The header names here (`sample_id`, `sample_type`, etc.) are the expected *source* column names as defined in the `field_mapping` section of the config.

*   `sample_id`
    *   **Purpose**: The unique identifier for this specific specimen.
*   `sample_type`
    *   **Purpose**: The type of specimen (e.g., `Plasma`, `Serum`, `Stool`).
*   `year_collected`
    *   **Purpose**: The year the specimen was collected.
*   `project`
    *   **Purpose**: The name of the project associated with this specimen.

---

### Mapping Configuration Example

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
