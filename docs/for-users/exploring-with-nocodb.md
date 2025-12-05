# Exploring Data with NocoDB

IDhub uses a tool called **NocoDB** to provide a user-friendly, web-based interface for viewing the data in the database. Think of it as a "spreadsheet view" of the live database.

This guide will walk you through the basics of navigating and exploring the data in NocoDB.

!!! note
    The NocoDB interface is for **viewing and querying** data. You should not use it to edit or delete records. All data modifications must go through the [ingestion process](../for-data-curators/ingestion-overview.md).

## Accessing NocoDB

You will be provided with a URL and login credentials to access the IDhub NocoDB instance. After logging in, you will see a list of "Projects" or "Bases," which correspond to different datasets within IDhub.

- [idhub.ibdgc.org](https://idhub.ibdgc.org)

---

## Navigating the Interface

Once you open a project, the interface will resemble a modern spreadsheet application.

### 1. Selecting Tables

On the left-hand sidebar, you will see a list of all the tables in the database. These are the fundamental datasets you can explore.

**Key Tables of Interest:**

*   `subjects`: The central table containing the list of all subjects and their Global Subject IDs (GSIDs).
*   `local_subject_ids`: A table that maps the local IDs from different centers to their corresponding GSID. This is useful for finding a subject's GSID if you only have their local ID.
*   `lcl`, `genotype`, `sequence`, `olink`, `specimen`, etc.: These are the sample and assay tables, each containing data about a specific type of biological sample or experimental result.

Click on any table name to open it in the main view.

### 2. The Grid View

When you select a table, the data is displayed in a grid (spreadsheet) view.

*   **Columns**: Each column represents a field in the database table (e.g., `global_subject_id`, `sample_type`, `date_collected`).
*   **Rows**: Each row represents a single record (e.g., one subject, one genotype record).

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
    1.  The **column** you want to filter on (e.g., `genotyping_project`).
    2.  The **operator** (e.g., `is`, `is not`, `contains`, `is empty`).
    3.  The **value** to compare against (e.g., `ProjectX`).

**Example: Find all genotype records from a specific project.**

1.  Open the `genotype` table.
2.  Click **Filter -> Add Filter**.
3.  Set the condition to: `genotyping_project` `is` `ProjectX`.
4.  The grid will update to show only the genotype records belonging to "ProjectX".

You can add multiple filter conditions to create more complex queries. For example, you could add a second condition: `AND` `quality_score` `is greater than` `8.5`.

### 3. Searching

For a quick lookup, use the **Search** bar, typically located at the top right of the view.

*   Enter a term you are looking for (e.g., a specific `sample_id` or `global_subject_id`).
*   NocoDB will search across all fields in the current table and show you the matching rows.

---

## Advanced Exploration and Further Help

Beyond sorting and filtering, NocoDB and IDhub provide more powerful tools for exploring data and getting help.

### Hiding and Reordering Columns

If a table has too many columns, you can simplify your view.

*   Click the **"Fields"** or **"Columns"** button at the top of the grid.
*   A list of all columns will appear. You can **drag and drop** to reorder them or click the **eye icon** to hide or show them. This only changes your personal view.

### Finding Documentation and Using the API

IDhub provides several built-in ways to find documentation or explore the data through a graphical API browser, which is a great alternative for users not comfortable with SQL.

We have created a dedicated guide that covers these features in detail:

> **[-> Read the Finding Help and Documentation Guide](./finding-help-and-documentation.md)**

This guide explains:
*   How to use NocoDB's global search (`Cmd+J`).
*   How to access NocoDB's official user guides.
*   How to use the **Swagger UI**, an interactive API browser for exploring IDhub data without writing any code.
