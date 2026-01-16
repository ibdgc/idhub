# IDhub Platform

**Integrated ID Hub for the IBD Genetics Consortium**

A comprehensive data integration and management platform for multi-center inflammatory bowel disease (IBD) research, providing centralized subject identification, data harmonization, and quality-controlled data pipelines.

---

## Overview

IDhub is a cloud-native platform that orchestrates the collection, validation, and integration of research data from multiple sources (REDCap projects, manual uploads, external systems) into a unified PostgreSQL database with a NocoDB frontend. The platform ensures data quality through automated validation, provides global subject identification (GSID) for cross-study linkage, and maintains complete audit trails for regulatory compliance.

### Key Capabilities

- **Global Subject Identification**: Collision-resistant GSID generation with intelligent identity resolution
- **Automated Data Pipelines**: REDCap synchronization, fragment validation, and database loading
- **Multi-Environment Support**: Separate QA and Production environments with CI/CD automation
- **Data Quality Assurance**: Schema validation, type checking, and referential integrity enforcement
- **Audit & Compliance**: Complete tracking of data lineage and transformation history
- **Flexible Integration**: REST APIs, S3-based staging, and configurable field mappings

---
