Run the project cost script and analyze the output for anomalies, spikes, and trends.

Steps:
1. Run `make project-costs` via Bash and capture the full output.
2. Analyze both tables (AWS and Databricks) and produce a written summary with these sections:

**AWS Costs**
- Total spend for the period and the single biggest cost driver
- Any day where spend was notably higher than the surrounding baseline — explain what service caused it and how much above average it was
- Trend: is spend flat, growing, or declining over the period?

**Databricks Costs**
- Total DBUs consumed (Jobs Serverless + SQL Serverless separately) and total storage DSUs
- The heaviest-spend days and what drove them (which SKU dominated)
- Any days with SQL Serverless activity — those indicate interactive/dashboard queries on top of scheduled jobs
- Trend: is DBU consumption flat, growing, or declining?

**Cross-cloud observation**
- Note whether AWS S3/egress spikes correlate with high Databricks DBU days (they should — heavy job runs produce more S3 I/O and egress)

Keep the analysis tight: one paragraph per section, specific numbers, no filler.
