# Sequence Data Template

This template provides an example of how to structure your CSV file for submitting sequencing data to the `fragment-validator`.

### CSV Data Example

```csv
consortium_id,center_name,sample_id,sample_type,vcf_sample_id
IDG-001-A,MSSM,SEQ-001,WGS,SAM-001A
IDG-002-B,Cedars-Sinai,SEQ-002,RNA-Seq,SAM-002B
IDG-003-C,Emory,SEQ-003,16S,SAM-003C
```

---

### Column Annotations

Below is a description of each column in the template and its purpose.

!!! note "Using Static Fields"
    For fields that have the same value across all records in your file (e.g., a project name that's consistent for the entire batch), you can define them as `static_fields` directly in your [mapping configuration](../creating-mapping-files.md) instead of including them as columns in your CSV. This simplifies your input file.

#### Subject Identification

*   `consortium_id`
    *   **Purpose**: The primary IBDGC identifier for a subject. This is used to find the correct subject in the database.


#### Center Identification

*   `center_name`
    *   **Purpose**: The name of the center where the data originated. This name is used to look up the correct center ID.

#### Sequence Data Fields

These columns map directly to the fields in the `sequence` table in ID Hub. The header names here are examples of common source column names.

*   `sample_id`
    *   **Purpose**: The unique identifier for this specific sequencing sample.
*   `sample_type`
    *   **Purpose**: The type of sequencing performed (e.g., `WGS`, `RNA-Seq`, `16S`).
*   `vcf_sample_id`
    *   **Purpose**: The sample identifier found within the VCF file, if applicable.

---

### Mapping Configuration Example

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
