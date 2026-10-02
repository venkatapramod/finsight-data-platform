# Security Policy

## Scope

FinSight is an educational data engineering and analytics project. It is not a production banking system and has not been independently security-audited.

## Reporting a security issue

If you discover a security issue, please avoid publicly posting credentials, personal information, or exploit details.

For private reporting, contact the repository maintainer using the email address listed in the README. Include:

- A concise description of the issue.
- The affected file or component.
- Steps to reproduce the issue, if safe to share.
- The potential impact and any suggested mitigation.

Do not include real customer records, passwords, API keys, or other secrets in your report.

## Security practices

- Never commit passwords, API keys, access tokens, or private configuration.
- Keep secrets in environment variables or untracked local configuration files.
- Use synthetic data in demonstrations and tests.
- Review third-party dependencies and keep them updated as appropriate.
- Restrict access to Kafka, Hadoop/HDFS, Hive, MongoDB, and Neo4j services to trusted users and networks.
- Do not use this project's fraud rules or risk scores to make real financial decisions.

## Supported versions

Security fixes are considered for the current version of the project maintained in the repository. No formal service-level agreement or guaranteed response time is provided.
