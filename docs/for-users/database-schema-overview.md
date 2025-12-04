# Database Schema Overview

The IDhub database is where all the validated and curated data is stored. Understanding the basic structure of the database will help you find the information you need. This guide provides a simplified overview of the most important tables and how they connect.

## The Subject-Centric Model

The entire database is designed around a **subject-centric model**. This means that almost every piece of data can be traced back to a unique individual, who is identified by a **Global Subject ID (GSID)**.

```mermaid
graph TD
    subgraph "Core Subject Data"
        A[subjects]
        B[local_subject_ids]
    end

    subgraph "Sample & Assay Data"
        C[genotype]
        D[sequence]
        E[specimen]
        F[...]
    end

    A -- "has GSID" --> B
    A -- "links to" --> C
    A -- "links to" --> D
    A -- "links to" --> E

    style A fill:#4CAF50,stroke:#333,stroke-width:2px
```

---

## Key Tables Explained

Here are the most important tables and what they contain.

### 1. `subjects`

This is the most important table in the database. It is the central registry for every individual.

*   **Purpose**: To store the master list of all subjects in IDhub.
*   **Key Columns**:
    *   `global_subject_id` (GSID): The unique, permanent identifier for a subject across all projects and centers. **This is the primary key you will use to link data together.**
    *   `center_id`: The ID of the center that first registered the subject.
    *   `withdrawn`: A flag indicating if the subject has withdrawn consent.
    *   `created_by`: The source system or process that created the subject record.

### 2. `local_subject_ids`

A single subject might have many different identifiers across different studies or clinical centers (e.g., a "consortium ID", a "MRN", a "site ID"). This table connects all of those local IDs back to a single GSID.

*   **Purpose**: To link various local identifiers to the one true Global Subject ID.
*   **Key Columns**:
    *   `global_subject_id`: The GSID the local ID belongs to.
    *   `local_subject_id`: The original identifier from the source system (e.g., `A000101-130001`).
    *   `identifier_type`: The type of local ID (e.g., `consortium_id`, `mrn`).
    *   `center_id`: The center that uses this local ID.

> **How to use this table**: If you have a local ID and need to find the subject's GSID, you can search this table for the `local_subject_id` to find the corresponding `global_subject_id`.

### 3. Sample & Assay Tables (`genotype`, `sequence`, `lcl`, `specimen`, etc.)

These tables contain information about the specific biological samples collected from subjects and the assays performed on them. Each table is dedicated to a specific data type.

*   **Purpose**: To store inventory, metadata, and results for different data types.
*   **Common Key Columns**:
    *   `global_subject_id`: The GSID of the subject from whom the sample was taken. This links the data back to the `subjects` table.
    *   A unique identifier for the specific data entry (e.g., `genotype_id`, `sample_id`).

#### Example: `genotype` Table

*   **Contains**: Information about a subject's genotype array data.
*   **Example Columns**: `genotype_id`, `genotyping_project`, `genotyping_barcode`.

#### Example: `lcl` Table

*   **Contains**: Information about Lymphoblastoid Cell Lines.
*   **Example Columns**: `knumber`, `niddk_no`, `passage_number`, `freeze_date`, `cell_line_status`.

---

## How the Tables Relate

Understanding the relationships between these tables is key to making sense of the data.

### Finding All Data for a Subject

You can find all the different types of data for a single subject by using their `global_subject_id`.

1.  Start with a `global_subject_id` from the `subjects` table.
2.  Use that GSID to search in the `genotype` table to find all their genotype records.
3.  Use that same GSID to search in the `sequence` table to find all their sequencing records.
4.  ...and so on for all other data tables.

This subject-centric design ensures that all data related to an individual can be easily aggregated and analyzed, even if it comes from different sources at different times.
