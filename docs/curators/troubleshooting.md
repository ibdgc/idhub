# Troubleshooting Data Ingestion

When you submit data for ingestion into IDhub, either through an automated pipeline or a manual file upload, you may encounter errors. This guide helps you understand and resolve common data-related issues.

Most errors are caught during the **Validation Stage**. When an error is found, the record is rejected and an error report is generated. Your main task will be to use this report to find the issue in your source data, correct it, and resubmit.

---

### Common Ingestion Errors & Solutions

#### Subject ID Resolution Failed

- **Symptom**: The error report says "Subject ID could not be resolved" or "No valid subject ID candidate found".
- **Meaning**: The system could not find a Global Subject ID (GSID) matching the subject identifiers provided in your record (e.g., `consortium_id`, `local_subject_id`). This error is rare because the system will create a new subject if one isn't found, but it can happen if the ID field itself is missing or malformed.
- **Solution**:
  1.  Check the record in your source file.
  2.  Ensure that at least one of the designated subject ID columns (like `consortium_id`) is present and contains a value.
  3.  Verify that the ID is correctly formatted and does not contain typos or extra characters. Correct the ID in your source data file.

!!! tip "Advanced Subject ID Troubleshooting"
	For complex identity resolution issues, you can directly query the `identity_resolutions` table or the `v_multi_gsid_conflicts` view to see exactly how the GSID service made its decision. Learn more in the [Guide to Audit & Resolution Tables](./audit-resolution-tables.md).

---

#### Missing Required Fields

- **Symptom**: The error report says "Required field 'sample_id' is missing".
- **Meaning**: A column that is mandatory for the target database table was empty or not included in your data file.
- **Solution**:
  1.  Identify the missing field from the error message.
  2.  Check your source file to see if the column is missing entirely or if the value in that specific row is empty.
  3.  Add the missing data to the row/column in your source file.

---

#### Invalid Data Type or Format

- **Symptom**: The error report says "Invalid date format for 'collection_date'" or "Value 'abc' is not a valid integer for 'passage_number'".
- **Meaning**: The data in a specific field does not match the expected data type.
- **Common Examples**:
  - Entering text (e.g., "N/A") in a number field.
  - Using an incorrect date format (e.g., `01/15/2024` instead of the expected `2024-01-15`).
  - Including non-numeric characters in a number field (e.g., `>5.0` instead of `5.0`).
- **Solution**:
  1.  Find the record and field mentioned in the error report.
  2.  Correct the value to match the expected format. For numeric fields, ensure there is only a number. For date fields, use the `YYYY-MM-DD` format. If a value is truly unknown, leave the cell blank rather than entering text like "Unknown".

---

#### Value Not in Allowed List (Enum Violation)

- **Symptom**: The error report says "Value 'Whole Blood' not in allowed values for 'sample_type'". The allowed values might be listed (e.g., `whole_blood`, `plasma`, `serum`).
- **Meaning**: You have provided a value for a field that only accepts a specific list of options, and your value isn't one of them.
- **Solution**:
  1.  Look at the list of allowed values provided in the error message.
  2.  Find the incorrect value in your source file.
  3.  Replace it with one of the valid options. Pay close attention to spelling, capitalization, and spacing (e.g., `whole_blood` is different from `Whole Blood`).

---

#### Duplicate Record or Key

- **Symptom**: The error report says "Duplicate natural key" or "A record with this 'sample_id' already exists".
- **Meaning**: You are trying to load a record that is supposed to be unique, but another record with the same unique identifier already exists in the database or even within the same file you are submitting.
- **Example**: You submit a file of `genotype` records, and two different rows have the same `genotype_id`. Or, you submit a `genotype_id` that was already loaded in a previous batch.
- **Solution**:
  1.  Investigate the duplicate. Is it a genuine error (e.g., a typo, a copy-paste mistake)? Or are you trying to _update_ an existing record?
  2.  If it's a mistake, correct the identifier in your source file to be unique.
  3.  If you are trying to update an existing record, the data submission process might be different. Consult with the system administrators. The system is designed to handle updates, but this error can occur if the identifying information is not a perfect match to the existing record.

---

### General Troubleshooting Steps

If you receive an error report, follow these steps:

1.  **Read the Error Message Carefully**: It usually tells you exactly what's wrong, which row it's in, and often what the expected value looks like.
2.  **Locate the Problem Row**: Use the record number or the data in the error report to find the exact row in your source file.
3.  **Correct the Data at the Source**: **Do not** edit the error report file. Always make corrections in your original CSV or Excel file. This ensures your source of truth is accurate for the future.
4.  **Resubmit**: Once you have corrected the errors, resubmit the entire file for validation again. The system will re-process it.
5.  **Ask for Help**: If you don't understand an error message or believe the error is incorrect, contact the IDhub system administrators. Provide them with the batch ID and the error message you received.

---

## Advanced Troubleshooting with Audit Tables

For particularly complex issues, or to understand the full history of a record, you can directly query the system's audit and resolution tables. These tables provide a transparent, detailed log of all data processing, validation, and loading activities.

We have created a detailed guide to help you understand and use these powerful tools.

> **[-> Read the Guide to Audit & Resolution Tables](./audit-resolution-tables.md)**
