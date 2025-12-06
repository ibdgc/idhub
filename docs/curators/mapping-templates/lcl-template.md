# LCL Data Template

This template provides an example of how to structure your CSV file for submitting Lymphoblastoid Cell Line (LCL) data to the `fragment-validator`.

### CSV Data Example

```csv
consortium_id,niddk_no,knumber,center_name
IDG-001-A,NIDDK-1111,K1111,MSSM
IDG-002-B,,K2222,Cedars-Sinai
,NIDDK-3333,K3333,Emory
```

---

### Column Annotations

Below is a description of each column in the template and its purpose.

!!! note "Using Static Fields"
    For fields that have the same value across all records in your file (e.g., a project name that's consistent for the entire batch), you can define them as `static_fields` directly in your [mapping configuration](../creating-mapping-files.md) instead of including them as columns in your CSV. This simplifies your input file.

#### Subject Identification

The validator uses one or more columns to find the correct subject in the database. For the LCL table, it will try the following columns in order. At least one of these must have a value for each row.


- `consortium_id`
  - **Purpose**: The primary IBDGC identifier for a subject.
  - **Identifier Type**: `consortium_id`
- `niddk_no`
  - **Purpose**: An alternative subject identifier (the NIDDK number). This column also contains the data for the `niddk_no` field in the LCL table itself.
  - **Identifier Type**: `niddk_no`

#### Center Identification

- `center_name`
  - **Purpose**: The name of the center where the data originated. This name is used to look up the correct center ID.
  - **Notes**: The system uses fuzzy matching and a list of aliases to find the correct center. For example, "MSSM", "Sinai", and "mount_sinai" will all resolve to the same center.

#### LCL Data Fields

These columns map directly to the fields in the `lcl` table in ID Hub.

- `knumber`
  - **Purpose**: The "K-number" identifier for the cell line.
- `niddk_no`
  - **Purpose**: The NIDDK number associated with the cell line. Note that this field serves a dual purpose: it's used as a subject ID candidate _and_ as data for the LCL record.

---

### Mapping Configuration Example

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
