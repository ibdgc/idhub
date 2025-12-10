# Security Guide

This document outlines the security architecture, best practices, and procedures for the IDhub platform. It is crucial to maintaining the integrity, confidentiality, and availability of sensitive biobank and clinical data.

## 1. Overview

!!! info "Security Overview"
    Security is a paramount concern for the IDhub platform. A multi-layered approach is adopted, encompassing network, application, database, and data security, to protect data throughout its lifecycle from ingestion to access.

## 2. Security Architecture

The IDhub security architecture is designed with defense-in-depth principles, meaning multiple security controls are layered throughout the system.

!!! info "Layered Security Approach"
    1.  **Network Security**: Focuses on protecting the network infrastructure and controlling access to the system.
    2.  **Application Security**: Ensures that the microservices and their APIs are robust against common vulnerabilities.
    3.  **Database Security**: Protects the persistence layer where critical data resides.
    4.  **Data Security**: Addresses the protection of data itself, both in transit and at rest.

    ```mermaid
    graph TD
        A[Client Request] --> B[External Network]
        B --> C[Firewall / Load Balancer]
        C --> D[Nginx Reverse Proxy]
        D --> E[Application Services (GSID, NocoDB, etc.)]
        E --> F[Database / S3 Storage]

        subgraph Security Layers
            D -- SSL/TLS, Rate Limiting, WAF --> E
            E -- API Key Auth, Input Validation --> F
            F -- RBAC, Encryption at Rest, Audit Logs --> G[Data]
        end
    ```

## 3. Authentication & Authorization

!!! abstract "API Key Authentication"
    Most IDhub microservices (e.g., GSID Service, Fragment Validator, Table Loader) use header-based API Key authentication for inter-service communication and external client access.

    -   **Mechanism**: A unique API key is generated and must be sent in the `X-API-Key` HTTP header for authenticated requests.
    -   **Management**: API keys are treated as sensitive secrets, stored securely as environment variables or in a secrets manager, and managed through the deployment process.

!!! abstract "NocoDB Authentication"
    NocoDB, as the primary web interface, manages its own user authentication. Access to bases and tables within NocoDB is governed by its internal role-based access control (RBAC) system.

    -   **Mechanism**: User/password authentication for NocoDB users.
    -   **Authorization**: NocoDB provides granular permissions for read/write access to individual tables and views.

!!! abstract "Service-Specific Authorization"
    Individual services implement authorization logic based on their specific requirements. For instance, the GSID service might validate if an API key has permissions to generate new GSIDs or only to resolve existing ones.

## 4. Network Security

!!! abstract "SSL/TLS Encryption"
    All external and internal (where possible) communication is encrypted using SSL/TLS to prevent eavesdropping and data tampering.

    -   **External Traffic**: Nginx handles SSL/TLS termination for all inbound HTTPS traffic (port 443).
        -   **Certificate Management**: Let's Encrypt is used for automatic certificate provisioning and renewal, ensuring up-to-date encryption.
    -   **Internal Traffic**: Communication between Nginx and backend services, and between services themselves, is configured to use HTTPS where feasible, or occurs within a secure, isolated Docker network.

!!! abstract "Firewall Rules"
    Strict firewall rules are applied to restrict network access to only essential ports and services.

    -   **Ingress**: Only ports 80 (HTTP, redirected to HTTPS) and 443 (HTTPS) are typically open to the public internet. SSH (port 22) is restricted to specific IP ranges or bastion hosts.
    -   **Egress**: Outgoing connections are limited to necessary external services (e.g., REDCap APIs, S3, monitoring endpoints).

!!! abstract "Nginx as a Reverse Proxy"
    Nginx acts as a central entry point, providing several network security benefits:

    -   **Traffic Filtering**: Can filter malicious requests and block known attack patterns.
    -   **Security Headers**: Adds HTTP security headers (HSTS, X-Frame-Options, CSP, etc.) to enhance browser security.
    -   **DDoS Protection**: Implements connection and request rate limiting to mitigate denial-of-service attacks.
    -   **IP Whitelisting/Blacklisting**: Can restrict access based on source IP addresses for sensitive endpoints.

## 5. Application Security

!!! abstract "Input Validation"
    All data ingress points (API endpoints, file uploads) perform rigorous input validation to prevent injection attacks (SQL injection, XSS) and ensure data integrity.

    -   **Pydantic**: FastAPI services leverage Pydantic for schema validation, ensuring incoming data conforms to expected types and structures.
    -   **Custom Logic**: Additional business logic validation is implemented within services (e.g., Fragment Validator).

!!! abstract "Secrets Management"
    Sensitive information such as API keys, database credentials, and other tokens are managed securely.

    -   **Environment Variables**: Secrets are injected into containers as environment variables at runtime, preventing them from being hardcoded in the codebase.
    -   **GitHub Secrets**: For CI/CD workflows, GitHub Secrets are used to store and inject credentials securely into build and deployment processes.
    -   **Never Log Secrets**: Application logs are configured to explicitly avoid logging sensitive information.

!!! abstract "Rate Limiting"
    Nginx is configured to apply rate limits to API endpoints and web interfaces, protecting against brute-force attacks and abuse.

    -   **Granularity**: Different rate limits can be applied based on the endpoint or client IP.
    -   **Burst Control**: Allows for short bursts of traffic while preventing sustained high-volume attacks.

!!! abstract "Dependency Management"
    Regular scanning and updating of third-party libraries and dependencies to mitigate known vulnerabilities.

    -   **Automated Scans**: Tools like Dependabot or similar are used to detect out-of-date or vulnerable dependencies.
    -   **Vulnerability Remediation**: Promptly addressing reported vulnerabilities in the software supply chain.

## 6. Database Security

!!! abstract "Role-Based Access Control (RBAC)"
    PostgreSQL is configured with distinct user roles, each granted the principle of least privilege.

    -   **Service Accounts**: Each microservice connects to the database using a dedicated user account with only the necessary permissions (e.g., the GSID service can read/write to subject-related tables but not to application configuration tables).
    -   **Admin Accounts**: Separate, highly restricted administrative accounts are used for database management.

!!! abstract "Prepared Statements"
    All database interactions in the application code use prepared statements or ORMs that prevent SQL injection vulnerabilities by separating SQL commands from user-supplied data.

!!! abstract "Encryption at Rest"
    The underlying PostgreSQL database and S3 buckets storing data fragments are configured to encrypt data at rest.

    -   **S3**: Server-Side Encryption (SSE) is utilized for all objects in S3 buckets.
    -   **PostgreSQL**: Volume-level encryption or database-specific encryption features are used to protect data stored on disk.

!!! abstract "Audit Logging"
    Database activity, including all INSERT, UPDATE, and DELETE operations, is logged to provide an immutable audit trail.

## 7. Data Security

!!! abstract "Encryption in Transit"
    Data is encrypted when it moves between components or services.

    -   **HTTPS**: All client-server and inter-service communication over public networks uses HTTPS.
    -   **Internal Networks**: Within the Docker Compose environment, services communicate over a private bridge network, minimizing exposure.

!!! abstract "Immutable S3 Fragments"
    Data fragments stored in S3 are immutable once written. This provides a robust audit trail and ensures that historical data cannot be tampered with.

    -   **Versioning**: S3 bucket versioning can be enabled to retain all versions of an object, providing an additional layer of protection against accidental deletion or modification.

## 8. Deployment Security

!!! abstract "Secure Environment Configuration"
    Deployment pipelines ensure that sensitive configuration data is never exposed.

    -   **Environment Files (`.env`)**: These files are generated dynamically during deployment using GitHub Secrets and are not committed to version control.
    -   **Hardened Containers**: Docker images are built with security best practices, minimizing installed software and running processes.

!!! abstract "Least Privilege for Services"
    Each Docker container and microservice is configured to run with the minimum necessary privileges.

    -   **Non-Root Users**: Services run as non-root users inside containers.
    -   **Restricted Mounts**: Volumes are mounted with read-only permissions where appropriate.

## 9. Best Practices

!!! tip "Regular Security Audits and Penetration Testing"
    Periodic security audits and penetration tests are conducted to identify vulnerabilities and ensure compliance with security standards.

!!! tip "Code Review"
    All code changes undergo peer review, including a focus on security considerations, before being merged into `main` or `prod` branches.

!!! tip "Incident Response Plan"
    A clear incident response plan is in place to effectively detect, respond to, and recover from security incidents.

!!! tip "Continuous Monitoring"
    Continuous monitoring of system logs, security events, and performance metrics for suspicious activities and potential threats.

## 10. Related Documentation

-   [System Architecture](architecture/overview.md)
-   [Deployment Guide](deployment-guide.md)
-   [Nginx Reverse Proxy Documentation](services/nginx.md)
-   [GSID Service Documentation](services/gsid-service.md)
-   [Database Schema](architecture/database-schema.md)