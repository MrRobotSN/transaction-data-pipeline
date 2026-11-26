# Data Quality, Governance & Security

**Objective:** Protect customer and transaction PII throughout the data lifecycle while ensuring trustworthy data for internal platforms and the external partner.

| Stage | Proposed controls | Trust and compliance |
|---|---|---|
| Ingestion | Secure SFTP or TLS, authenticated partner access, S3 encryption using AWS KMS and source-file validation. | Protect data in transit and at rest; verify source integrity. |
| Processing | Enforce data contracts, validate required fields and data types, deduplicate, standardise timestamps and quarantine invalid records. Restrict access to rejected data. | Prevent inaccurate transactions from affecting loyalty processing and protect sensitive records. |
| Storage | Classify PII in the Glue Data Catalog. Apply least-privilege IAM roles, Lake Formation tag-based and column-level permissions, encryption and retention policies. Restrict direct S3 access. | Prevent unauthorised access and unnecessary retention. |
| Consumption | Expose approved datasets through restricted views; mask or tokenise customer identifiers where possible. Share only necessary fields with the partner through an authenticated, auditable channel. | Support purpose limitation and data minimisation. |
| Monitoring | Capture access and changes through CloudTrail and appropriate data-access logs. Record source file, pipeline run, target version and reconciliation results. Alert on quality failures and suspicious access. | Enable traceability, accountability and incident investigation. |

## Governance and accountability

Agree on permitted data use, retention, access reviews and incident responsibilities with the partner. Apply POPIA principles where applicable, with approved deletion and incident-response procedures.

Data engineers own the implementation, automated tests and monitoring of these controls alongside data owners and security teams. Publish data only after reconciliation and agreed quality checks pass.

The local PySpark pipeline demonstrates validation and quarantine. Cloud security and governance controls are proposed, not deployed.
