# Worked example — `reports/cost/2026-07-22.md`

A committed 30-day report, force-added past the `reports/cost/` gitignore so one finished example
survives. Read the file itself; this page is about *why* its Analysis section is written the way it
is. Do not copy its numbers into a new report — they are a snapshot of one window.

## What the numbers were

$39.81 total: Databricks $39.43 (99.0%) at list, AWS $0.38 (1.0%). Jobs Serverless 68.09 DBU
($23.83), SQL Serverless 21.57 DBU ($15.10), storage 20.66 DSU ($0.48). Attributed to jobs: $21.49
of $39.43.

## What the analysis did with them, and why

**It led with the split, not with AWS.** $0.38 of AWS is noise; opening with it would bury the fact
that the entire cost conversation is about DBUs. Section order in the report follows the file;
narrative order should follow the money.

**It converted a spike to dollars before judging it.** The AWS week of 2026-07-13 is ~5× the
surrounding baseline, which sounds alarming until you say it is $0.1925. Ratios without dollars
mislead on a project this small.

**It used the daily `<details>` block to attribute the spike to a date.** The weekly pivot only says
"that week"; the daily block pinned it to 2026-07-16 and identified Cost Explorer API calls as the
driver — i.e. the report measuring itself, since Cost Explorer bills per request. That conclusion is
unreachable from stdout alone, which is why the skill insists on reading the report file.

**It normalized the edge weeks before claiming a trend.** Raw weekly totals suggested a decline;
per-day Jobs Serverless ($1.22/day → $0.42/day → ~$0.72/day) showed a stable baseline with one
late-June spike instead. Opposite conclusion, same data.

**It treated SQL Serverless silence as a finding.** Two and a half weeks at exactly $0.00, then
$2.90 and $3.58 on two days. The absence is the signal — nothing scheduled touches the warehouse and
nobody opens the dashboard on an ordinary day — and at $15.10 it is the second most expensive thing
in the project despite running about five days out of thirty.

**It reconciled before trusting the per-job table.** $21.49 attributed against $39.43 total, with
the ~$17.94 gap explained by the SQL warehouse carrying no `job_id`. Because that reconciles, the
breakdown is trustworthy *as a picture of scheduled work only* — stated explicitly rather than
letting the reader assume it covers everything.

**It compared per active day, never raw totals.** prod ran 31 days at $0.46/day; staging ran 5 days
at $1.35/day — 2.9× prod's daily burn, invisible in the raw column where prod looks far more
expensive. The `Days` column exists for exactly this.

**It found the batch-vs-SDP gap and argued it was real.** `job1_prod` $6.58 vs `job1_sdp_prod` $4.03
for the same medallion tables, repeated in staging ($2.48 vs $1.62). A 35–39% gap holding over 31
days *and* across two environments is what separates a finding from noise — the durability is the
argument, not the single number.

**It checked the integration tests.** `job1_prod_integration` at $3.78 is 57% of the pipeline it
validates, and in staging the integration test cost *more* than the job under test. Easy to skim
past; the skill calls them out because of this.

## The shape to reproduce

One paragraph per section, every claim carrying its number, comparisons normalized before they are
made, and absences reported as findings. No filler, no restating tables that are already in the file
directly above.
