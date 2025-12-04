# Example SQL Queries

For users who are comfortable with SQL, you can often run queries directly through the NocoDB interface or by connecting to the database with a compatible SQL client.

Here are some common and useful queries to help you get started.

!!! warning
    These queries are for **read-only** (`SELECT`) operations. You do not have permission to modify data (`INSERT`, `UPDATE`, `DELETE`). Please exercise caution and ensure your queries are efficient to avoid impacting system performance.

---

### 1. Find a Subject's GSID Using a Local ID

This is one of the most common tasks: you have a local or consortium ID and you need to find the official Global Subject ID (GSID).

```sql
SELECT
  global_subject_id,
  center_id,
  identifier_type,
  created_at
FROM
  local_subject_ids
WHERE
  local_subject_id = 'A000101-130001';
```

**What it does:**
*   Searches the `local_subject_ids` table.
*   Finds the row matching the `local_subject_id` you provided.
*   Returns the corresponding `global_subject_id`.

---

### 2. Get All Local IDs for a Known GSID

This is the reverse of the query above. If you have a GSID, you can find all the different local IDs associated with that person from various centers or studies.

```sql
SELECT
  local_subject_id,
  identifier_type,
  center_id
FROM
  local_subject_ids
WHERE
  global_subject_id = 'GSID-1KKG7NR2Z4XB2A2J';
```

---

### 3. Count Samples of Each Type for a Specific Subject

This query counts how many DNA, RNA, and LCL samples are available for a given subject.

```sql
SELECT
  'dna' AS sample_type,
  COUNT(*) AS count
FROM
  dna
WHERE
  global_subject_id = 'GSID-1KKG7NR2Z4XB2A2J'

UNION ALL

SELECT
  'rna' AS sample_type,
  COUNT(*) AS count
FROM
  rna
WHERE
  global_subject_id = 'GSID-1KKG7NR2Z4XB2A2J'

UNION ALL

SELECT
  'lcl' AS sample_type,
  COUNT(*) AS count
FROM
  lcl
WHERE
  global_subject_id = 'GSID-1KKG7NR2Z4XB2A2J';
```

---

### 4. Find High-Quality DNA Samples

This query finds DNA samples that meet certain quality criteria, such as a high concentration and a good purity score (A260/280 ratio).

```sql
SELECT
  global_subject_id,
  sample_id,
  concentration_ng_ul,
  quality_score -- Or the correct name for the purity ratio column
FROM
  dna
WHERE
  concentration_ng_ul > 50
  AND quality_score BETWEEN 1.8 AND 2.0
ORDER BY
  concentration_ng_ul DESC
LIMIT 100;
```
*Note: Column names like `quality_score` might vary. Check the table schema in NocoDB for the exact name.*

---

### 5. Find Subjects Registered by a Specific Source System

This query helps you find subjects that were created by a particular data pipeline or source.

```sql
SELECT
  global_subject_id,
  center_id,
  created_at,
  created_by
FROM
  subjects
WHERE
  created_by = 'redcap_pipeline'
ORDER BY
  created_at DESC
LIMIT 500;

```

**What it does:**
*   Looks at the `subjects` table.
*   Filters for records where the `created_by` field is `redcap_pipeline`.
*   Shows the 500 most recently created subjects from that source.

---

### Tips for Querying

*   **Start with `LIMIT`**: When exploring a table for the first time, always add `LIMIT 10` to the end of your query. This prevents you from accidentally trying to load millions of rows, which can be slow.
*   **Use `WHERE` to filter**: Be as specific as possible with your `WHERE` clauses to narrow down the data you are looking for. This is much more efficient than retrieving a large dataset and filtering it on your own machine.
*   **Use NocoDB's Filters First**: Before writing your own SQL, try to achieve your goal using NocoDB's built-in [Sort and Filter](../for-users/exploring-with-nocodb.md#2-filtering) features. They are powerful and safe to use.
*   **Check Column Names**: SQL is precise. If your query fails, double-check that the column and table names are spelled correctly. You can see the exact names in NocoDB's grid view.
