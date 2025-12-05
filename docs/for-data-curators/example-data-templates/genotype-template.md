# Genotype Data Template

This template provides an example of how to structure your CSV file for submitting genotype data to the `fragment-validator`.

### CSV Data Example

```csv
consortium_id,identifier_type,center_name,id,project,barcode
IDG-001-A,consortium_id,MSSM,GENO-001,GSA-Array-v1,987654321
IDG-002-B,consortium_id,Cedars-Sinai,GENO-002,GSA-Array-v1,987654322
IDG-003-C,consortium_id,Emory,GENO-003,GSA-Array-v2,987654323
```

---

### Column Annotations

Below is a description of each column in the template and its purpose.

!!! note "Using Static Fields"
    For fields that have the same value across all records in your file (e.g., a project name that's consistent for the entire batch), you can define them as `static_fields` directly in your [mapping configuration](./creating-mapping-files.md) instead of including them as columns in your CSV. This simplifies your input file.

#### Subject Identification

*   `consortium_id`
    *   **Purpose**: The primary IBDGC identifier for a subject.
*   `identifier_type`
    *   **Purpose**: Specifies what *type* of ID is in the `consortium_id` column for that row. For this mapping, it will typically be `consortium_id`.


#### Center Identification

*   `center_name`
    *   **Purpose**: The name of the center where the data originated. Although the current `genotype_mapping.json` does not specify a `center_id_field`, providing this column allows for future-proofing and consistency.

#### Genotype Data Fields

These columns map directly to the fields in the `genotype` table in ID Hub. The header names here (`id`, `project`, `barcode`) are the expected *source* column names as defined in the `field_mapping`.

*   `id`
    *   **Purpose**: The unique identifier for this specific genotype record. This will be mapped to the `genotype_id` column in the database.
*   `project`
    *   **Purpose**: The name of the genotyping project (e.g., `GSA-Array-v1`). This maps to the `genotyping_project` column.
*   `barcode`
    *   **Purpose**: The barcode of the genotyping array. This maps to the `genotyping_barcode` column.

---

### Mapping Configuration Example

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
