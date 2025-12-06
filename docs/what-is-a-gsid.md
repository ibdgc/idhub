# What is a Global Subject ID (GSID)?

A Global Subject ID (GSID) is a unique identifier assigned to every subject whose data is integrated into IDhub. It serves as a **surrogate key**, creating a common, stable link across all the different subject and sample data sources that are collected and harmonized within the resource.

## Purpose and Scope

Different institutions and studies often have their own internal standards and practices for identifying subjects. The primary purpose of the GSID is to harmonize these varied identifiers within a single, centralized resource.

It is critical to understand the scope and limitations of the GSID:

-   **Relevance is Internal to IDhub**: GSIDs are exclusively meant to act as a resolving agent to establish relative links *within* the IDhub. They have no meaning or relevance outside of this system.
-   **Not a Replacement for Existing IDs**: GSIDs **do not** replace existing subject identifiers used across the consortium or at individual research centers (e.g., `consortium_id`, site-specific IDs). These original identifiers are preserved and mapped to the GSID.
-   **Not Permanent or Archival**: Unlike identifiers from a permanent resolving tool (like a `consortium_id`), GSIDs are considered ephemeral. They are subject to being completely wiped and regenerated following major data curation events or pipeline updates. They should not be used for long-term data tracking outside of IDhub.

In summary, think of the GSID as a temporary, internal "glue" that holds a subject's disparate data together within IDhub, but which should not be carried forward into other data management systems or used as a permanent identifier in your own research records.
