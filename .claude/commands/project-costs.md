Run the project cost script, analyze the output for anomalies, spikes and trends, and write the
analysis into the generated markdown report.

## Steps

1. Run `make project-costs` via Bash and capture the full output. It prints to stdout **and**
   writes `reports/cost/YYYY-MM-DD.md` (gitignored) containing every table in markdown, ending with
   an empty `## Analysis` section. The script writes only numbers, never prose.
2. Analyze the output (see below).
3. **Edit `reports/cost/YYYY-MM-DD.md` and replace the `## Analysis` placeholder with your written
   analysis.** Do not retype the tables — they are already in the file, and re-transcribing numbers
   risks introducing errors. Reference them instead.
4. Report the same analysis back in chat, and link the report path.

If `make project-costs` fails to reach Databricks with an auth error, the `dev` profile's OAuth
refresh token has expired. Tell the user to run `! databricks auth login --profile dev` in the
session, then re-run. Do not attempt the browser login yourself.

## What the script emits

Four tables, and only four:

1. **AWS by Week × Service** (USD, Total column)
2. **Databricks by Week × SKU** (USD at list price, Total column)
3. **Combined Totals by Service** across both clouds — native `Quantity`/`Unit` plus a totalled
   `USD` column. This is the only place native DBU/DSU/GB quantities appear.
4. **Databricks by Job / Pipeline** — one row per job or SDP pipeline that incurred spend, with
   `Kind`, native `Quantity`/`Unit`, `USD` and `Days` (distinct days with usage). In the generated
   report it sits directly after Combined Totals; on stdout it prints last.

Databricks usage is monetized by joining `system.billing.list_prices` inside the script, so DBU/DSU
/GB and AWS dollars are directly comparable.

**Per-day data is not printed to stdout** — it lives only in the collapsed `<details>` blocks of the
generated report. You will need it: the weekly pivots average single-day spikes away, and those
blocks are the only place a spike can be attributed to a date. Read the report file, don't ask the
script for a daily table.

## Analysis sections

**Lead with the combined USD total and the split between clouds.** On this project Databricks is
~99% of spend and AWS is rounding error, so an AWS-first summary buries the story. Convert spikes
to dollars before calling them big or small.

**AWS Costs**
- Total spend for the period and the single biggest cost driver
- Any week notably above the surrounding baseline — name the service responsible, and open the
  report's daily-detail block to identify which day(s) within that week actually caused it
- Trend: is spend flat, growing, or declining week over week?

**Databricks Costs**
- Total USD at list price, and the DBU/DSU quantities behind it (Jobs Serverless + SQL Serverless
  separately, plus storage DSUs)
- The heaviest weeks and what drove them (which SKU dominated), plus the peak day within them
- Any weeks with SQL Serverless activity — those indicate interactive/dashboard queries on top of
  scheduled jobs. Call out stretches with *no* SQL activity too; a long-silent warehouse may mean a
  dashboard nobody opens.
- Trend: is DBU consumption flat, growing, or declining?

**By job / pipeline**
- The top spenders, and the prod / staging / dev split — prod runs daily, staging and dev only on
  the days someone deployed, so compare *per active day*, never raw totals. The `Days` column is
  what makes that comparison possible.
- Compare `job1_*` against its `job1_sdp_*` counterpart: they produce the same medallion tables by
  different execution models, so a persistent gap between them is a real finding, not noise.
- Watch the integration-test jobs. They are easy to overlook and can rival the pipeline they test.
- Reconcile before trusting: the attributed total is always *less* than the Databricks total, since
  SQL warehouse and interactive compute carry no `job_id`. The note under the table gives the
  attributed share — if it moves a lot between runs, interactive usage changed, not the jobs.

**Cross-cloud observation**
- Note whether AWS S3/egress spikes correlate with high Databricks DBU days (they should — heavy
  job runs produce more S3 I/O and egress). Call out any that *don't* correlate, in either
  direction; those are the interesting ones. Expect S3 to track Jobs Serverless specifically, not
  total DBU — SQL warehouse queries read Databricks-managed storage and won't move S3.
- Remember serverless SKUs are all-in: Databricks bills the compute inside the DBU rate rather than
  charging the AWS account for EC2. AWS spend is therefore a *proxy for job activity*, never a
  measure of pipeline cost.

## Caveats to respect

- **Databricks USD is list price.** `list_prices` excludes account discounts and commit contracts,
  so it is an upper bound. Say "at list price" rather than implying it is the invoice.
- **Partial edge weeks.** The first and last weeks of the window are almost always short. Never
  read a trend off their raw totals — normalize to per-day before claiming a rise or fall, and say
  so when it changes the conclusion.
- **Estimated days.** AWS weeks flagged `*` contain not-yet-finalized costs.
- **Quantity never sums.** DBU, DSU and GB are not comparable, so the `Quantity` column is not
  totalled — only `USD` is. Don't total quantities across SKUs; they bill at different rates.
- **Zero-usage services are filtered** out of the combined table.
- Sanity-check that a weekly pivot's Total column reconciles with the combined table before
  trusting a spike.

Keep the analysis tight: one paragraph per section, specific numbers, no filler.
