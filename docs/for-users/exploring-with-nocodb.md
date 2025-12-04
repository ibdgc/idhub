# Exploring Data with NocoDB

IDHub uses a tool called **NocoDB** to provide a user-friendly, web-based interface for viewing the data in the database. Think of it as a "spreadsheet view" of the live database.

This guide will walk you through the basics of navigating and exploring the data in NocoDB.

!!! note
    The NocoDB interface is for **viewing and querying** data. You cannot use it to edit or delete records. All data modifications must go through the official [ingestion process](../for-data-curators/ingestion-overview.md).

## Accessing NocoDB

You will be provided with a URL and login credentials to access the IDHub NocoDB instance. After logging in, you will see a list of "Projects" or "Bases," which correspond to different datasets within IDHub.

---

## Navigating the Interface

Once you open a project, the interface will resemble a modern spreadsheet application.

### 1. Selecting Tables

On the left-hand sidebar, you will see a list of all the tables in the database. These are the fundamental datasets you can explore.

**Key Tables of Interest:**

*   `subjects`: The central table containing the list of all subjects and their Global Subject IDs (GSIDs).
*   `local_subject_ids`: A table that maps the local IDs from different centers to their corresponding GSID. This is useful for finding a subject's GSID if you only have their local ID.
*   `lcl`, `dna`, `rna`, `specimen`, etc.: These are the sample tables, each containing data about a specific type of biological sample.

Click on any table name to open it in the main view.

### 2. The Grid View

When you select a table, the data is displayed in a grid (spreadsheet) view.

*   **Columns**: Each column represents a field in the database table (e.g., `global_subject_id`, `sample_type`, `date_collected`).
*   **Rows**: Each row represents a single record (e.g., one subject, one DNA sample).

![NocoDB Grid View](https://www.nocodb.com/images/v2/smart-spreadsheet/spreadsheet-redefined-light.png)
*(Image courtesy of NocoDB. The IDHub interface may look slightly different.)*

---

## Finding and Filtering Data

NocoDB provides powerful tools to help you find the specific data you need without having to scroll through thousands of rows.

### 1. Sorting

You can sort the entire table by the values in a specific column.

*   Click the **down arrow** icon in a column header.
*   Select **"Sort Ascending"** (A-Z, 1-100) or **"Sort Descending"** (Z-A, 100-1).
*   A small arrow will appear in the column header to indicate that the table is being sorted by that column.

### 2. Filtering

Filtering allows you to show only the rows that meet certain criteria. This is one of the most useful features for exploring data.

*   Click the **"Filter"** button, usually located at the top of the grid view.
*   Click **"Add Filter"**.
*   Build your filter condition by choosing:
    1.  The **column** you want to filter on (e.g., `sample_type`).
    2.  The **operator** (e.g., `is`, `is not`, `contains`, `is empty`).
    3.  The **value** to compare against (e.g., `whole_blood`).

**Example: Find all DNA samples from a specific project.**

1.  Open the `dna` table.
2.  Click **Filter -> Add Filter**.
3.  Set the condition to: `project` `is` `GAP-2`.
4.  The grid will update to show only the DNA samples belonging to the "GAP-2" project.

You can add multiple filter conditions to create more complex queries. For example, you could add a second condition: `AND` `quality_score` `is greater than` `8.5`.

### 3. Searching

For a quick lookup, use the **Search** bar, typically located at the top right of the view.

*   Enter a term you are looking for (e.g., a specific `sample_id` or `global_subject_id`).
*   NocoDB will search across all fields in the current table and show you the matching rows.

---

## Advanced Usage

### Hiding and Reordering Columns

If a table has too many columns, you can simplify your view.

*   Click the **"Fields"** or **"Columns"** button at the top of the grid.
*   A list of all columns will appear. You can **drag and drop** to reorder them.
*   Click the **eye icon** next to a column name to hide or show it.

This does not change the underlying data, only your personal view of it.

### Querying with the API

For programmatic access, NocoDB provides a REST API that allows you to fetch data. You can often find the API endpoint and required filters directly within the NocoDB UI, which helps in constructing automated queries.

Please refer to the official NocoDB documentation or contact a system administrator for more details on using the API.
