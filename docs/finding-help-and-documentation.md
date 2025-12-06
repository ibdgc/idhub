# Finding Help and Documentation

While exploring IDhub, you have several powerful, built-in tools at your disposal for searching, understanding the data, and interacting with the system's API without writing any code. This guide points you to these resources, which are especially useful for users who prefer a graphical interface over command-line or SQL-based tools.

---

### 1. NocoDB Global Search (`Cmd+J`)

NocoDB has a powerful global search feature that acts like a command palette, allowing you to quickly find data or navigate the documentation.

*   **How to access**:
    *   Click the **"Search documentation"** button in the upper-left navigation panel.
    *   Or, use the keyboard shortcut: **`Cmd+J`** (on macOS) or **`Ctrl+J`** (on Windows).

*   **What it does**:
    *   Lets you search for specific tables or views.
    *   Searches the content of NocoDB's own documentation, which can be helpful for general questions about the interface.

![NocoDB Search](https://nocodb.com/images/features/command-palette.webp)
*Image from NocoDB's official documentation.*

---

### 2. NocoDB Official Resources (Help Button)

For general questions about how to use the NocoDB interface (e.g., how to sort, filter, or create views), the official NocoDB documentation is the best place to look. IDhub provides a direct link to these resources.

*   **How to access**:
    *   Click the **question mark icon (`?`)** in the bottom-left navigation panel.
    *   This opens a menu with links to NocoDB's official **Product Docs** and **API Docs**.

*   **What it's for**:
    *   **Product Docs**: The complete user manual for NocoDB. Use this to understand features like filtering, sorting, creating views, and more.
    *   **API Docs**: Technical documentation for NocoDB's general-purpose API.

> By directing you to the official docs, we ensure you get the most accurate, up-to-date information directly from the creators of the tool, avoiding duplicated and potentially outdated guides.

---

### 3. IDhub API Browser (Swagger UI)

For users who want to explore the data via an API but are not comfortable writing code or SQL, IDhub provides an interactive API browser using Swagger UI. This interface is a user-friendly way to "query" the database through web requests.

*   **How to access**:
    1.  Click the dropdown menu next to the base name (e.g., "IBDGC IDhub") in the upper-left corner.
    2.  Select **"Swagger UI"** from the menu.
    3.  This will open a new tab with an interactive list of all available IDhub API endpoints.

*   **What it does**:
    *   It provides a **graphical interface** for every table in the database.
    *   You can click on an endpoint (e.g., `/api/v1/db/data/v1/IBDGC-IDhub/genotype`) to expand it.
    *   It shows you all the available parameters for filtering, sorting, and limiting your query.
    *   You can fill in these parameters in the web form and click **"Execute"** to see the live data returned from the database, directly in your browser.

This is a powerful tool for exploring the data schema and testing queries without needing to connect to the database directly. It's a great stepping stone between the simple grid view and writing complex SQL.
