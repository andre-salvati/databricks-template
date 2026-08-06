# Data divergence — `prod` batch vs SDP medallion paths

**Date:** 2026-08-06
**Investigator:** Claude Code, via the `data-divergence` skill
**Sides:** `prod.curated.order_enriched` (batch, `job1`) vs `prod.curated.order_enriched_sdp` (declarative, `job1_sdp`)
**Grain:** one row per `(order_id, item_seq)`
**Identity:** all queries run through the `databricks` MCP server, which authenticates as `template-sp` — the same
service principal `prod` runs as. Reads only; nothing in this investigation wrote to `prod`.

---

## Verdict

The two silver tables hold the same orders but disagree on **two columns** and **two blocks of rows**. Four
distinct divergences, three of them still live:

| # | Divergence | Scope | Status | Cause |
|---|---|---|---|---|
| 1 | `order_date` shifted **13 days earlier** in SDP | 6,000,000 rows | **Live** | Initial seed anchored on run date; SDP froze the pre-reseed values |
| 2 | `country` differs | 5,364,000 rows / 447 of 500 customers | **Live** | Same mechanism — an unintended freeze |
| 3 | 5,000 orders of 2026-07-24 missing from **batch** | 15,000 rows | **Live** | Failed run + date-scoped incremental MERGE |
| 4 | 5,000 orders of 2026-06-24 **tripled** in SDP | 15,000 rows | Live, cosmetic | Source double-append; DQX caught it on the batch side only |

Divergences 1 and 2 share one root cause and one moment. Divergence 3 is unrelated and is the residue of an
already-fixed outage. Divergence 4 is a data-quality asymmetry that is arguably working as designed.

**The transform is not at fault.** `generate_orders.py:25` and `job1_sdp/transforms.py:49` derive the date with
byte-identical expressions:

```python
df_order["date"].cast("date").alias("order_date")
```

Every other column reconciles exactly: `product_name`, `order_total`, `item_total`, `item_quantity` all show
**zero** mismatches across 6.2M joined rows. This is not corruption, a broken join, or a bad filter. It is two
tables that froze the same upstream at two different moments.

---

## 1. Counting down both paths

```
| layer                       | rows      | distinct keys | min date   | max date   |
|-----------------------------|-----------|---------------|------------|------------|
| external_source.order       | 2,225,000 | 2,220,000     | 2025-06-27 | 2026-08-06 |
| raw.order        (batch)    | 2,215,000 | 2,215,000     | 2025-06-27 | 2026-08-06 |
| raw.order_sdp    (SDP)      | 2,225,000 | 2,220,000     | 2025-06-27 | 2026-08-06 |
| curated.order_enriched      | 6,215,000 | 2,215,000     | 2025-06-27 | 2026-08-06 |
| curated.order_enriched_sdp  | 6,230,000 | 2,220,000     | 2025-06-14 | 2026-08-06 |
| report.order_agg            |   202,560 |               | 2025-06-27 | 2026-08-06 |
| report.order_agg_sdp        |   203,500 |               | 2025-06-14 | 2026-08-06 |
```

Read this top to bottom and the shape of the problem is already visible:

- **Bronze agrees on dates.** Both `raw.order` and `raw.order_sdp` start at 2025-06-27. The 13-day gap appears
  for the first time at **silver**, and only on the SDP side. Everything upstream is exonerated by this one query.
- **`raw.order` is 10,000 rows lighter** than its source and has no duplicate ids, while `raw.order_sdp` copies
  the source verbatim. That is DQX: the batch path quarantines, the SDP path does not. → divergence 4.
- **Gold faithfully propagates.** `report.order_agg_sdp` is a materialized view; it recomputed correctly *from
  wrong inputs* and inherited the shifted window. A correct aggregation over frozen bad data is still bad data.

The min/max pattern is the classic **"both edges shifted, middle identical"** signature: nothing is missing,
a derived column moved.

---

## 2. Diffing by key

Joining the two silvers on `(order_id, item_seq)` and histogramming the date delta:

```
| delta_days |         n | name_diff | total_diff | otot_diff | country_diff | qty_diff |
|------------|-----------|-----------|------------|-----------|--------------|----------|
|         13 | 6,000,000 |         0 |          0 |         0 |    5,364,000 |        0 |
|          0 |   225,000 |         0 |          0 |         0 |            0 |        0 |
```

One bucket holds **every** shifted row, at exactly 13 days. A single-bucket histogram means a **formula**, not
corruption — a constant offset applied uniformly. And 6,000,000 is not an arbitrary number: it is precisely the
initial backfill (2,000,000 orders × 3 items). The 225,000 rows that agree are everything appended since.

The `country_diff` column was the surprise. It was not part of the original complaint, and it rides on exactly
the same 6,000,000 rows.

---

## 3. Pinning the blast radius with `row_commit_version`

Time travel is useless here — the events are 43 days old and `delta.deletedFileRetentionDuration` is 168 hours.
`_metadata.row_commit_version` is not retention-bounded, and it attributes live rows to the commit that wrote them:

```
| commit |         n | min date   | max date   |
|--------|-----------|------------|------------|
|      3 | 6,000,000 | 2025-06-14 | 2026-06-11 |   ← every bad row, one commit
|      5 |     5,000 | 2026-06-24 | 2026-06-24 |
|      6 |    10,000 | 2026-06-24 | 2026-06-24 |
|      7 |     5,000 | 2026-06-25 | 2026-06-25 |
|    ... |     5,000 | one per day, correct    |
|     51 |     5,000 | 2026-08-06 | 2026-08-06 |
```

All 6,000,000 shifted rows land in **commit 3**, and **commit 5 onward is already correct**. That converts the
theory into a fact: this was a single bad write, not an ongoing drift. Nothing since 2026-06-24 12:26 has been
wrong, and nothing will retroactively fix commit 3.

---

## 4. The timeline

Delta history across four objects, all on **2026-06-24**:

```
12:09:32  external_source.order      v0  CREATE TABLE AS SELECT              0 rows   ← empty bootstrap
12:09:51  external_source.customer   v1  CREATE OR REPLACE TABLE AS SELECT   500      ← countries reset to seed banding
12:09:58  external_source.order      v3  CREATE OR REPLACE TABLE AS SELECT   2,000,000 ← _seed_initial, anchored 2026-06-24
12:12:00  curated.order_enriched     v0  CREATE OR REPLACE TABLE AS SELECT   6,000,000 ← batch first_run branch
12:14:51  curated.order_enriched_sdp v0  CREATE TABLE                                 ← SDP streaming table created
12:15:22  curated.order_enriched_sdp v3  STREAMING UPDATE                    6,000,000 ← the bad commit
12:19:39  external_source.order      v5  WRITE / Append                      5,000    ← 2026-06-24 incremental
12:21:45  curated.order_enriched     v4  MERGE                               5,000
12:26:23  curated.order_enriched_sdp v5  STREAMING UPDATE                    5,000    ← correct from here on
15:55:05  external_source.order      v7  WRITE / Append                      5,000    ← same 5,000 ids AGAIN
15:56:45  curated.order_enriched_sdp v6  STREAMING UPDATE                   10,000
15:57:24  curated.order_enriched     v8  MERGE                                   0    ← DQX quarantined them
```

`external_source.order` v0 is 2026-06-24 12:09:32. **That is when the table was created, not when the data
began** — prod was dropped and rebuilt from scratch that morning, and the previous incarnation's history is
gone with it. The reseed re-anchored 363 days of synthetic history to a new `seed_date`.

Batch silver was written at **12:12:00**, three minutes *before* SDP silver at **12:15:22** — yet batch got
2025-06-27 and SDP got 2025-06-14. The two sides read the same source table minutes apart and disagree by
13 days, and 2026-06-24 − 13 days = **2026-06-11**, the max date in SDP commit 3. The SDP path read the
**previous incarnation's** data.

### What is proven and what is inference

**Proven by data:** the reseed at 12:09:58; the two silver writes and their contents; that all bad rows sit in
one commit; that the prior window was anchored on 2026-06-11; that the transform is identical; that bronze is
now consistent while SDP silver is not.

**Inference:** that the SDP pipeline's first update consumed a **stale materialization** of `raw.order_sdp`
dating from before the 12:09:58 reseed. This cannot now be confirmed. `raw.order_sdp` is a materialized view,
and `DESCRIBE HISTORY` refuses it:

```
[EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE] 'DESCRIBE HISTORY' expects a table but
`prod`.`raw`.`order_sdp` is a view.
```

That refusal is itself informative — it tells us the object whose intermediate state we need is precisely the
one whose history we cannot read. The pipeline event log would settle it, but the repo configures no permanent
event log table, so those events aged out with the default retention. **The mechanism is the best-fitting
explanation for the evidence, not a proven fact.** The 13-day arithmetic and the single-commit blast radius are
facts regardless of which mechanism delivered the stale rows.

---

## 5. Root cause: a date that is not a fact

`src/template/job1/seed_sources.py:135`, in `_seed_initial`:

```python
F.date_sub(F.lit(seed_date), (F.col("id") % 363).cast(IntegerType())).cast("string").alias("date"),
```

The entire 2M-order backfill spans "the 363 days before `seed_date`". **`seed_date` is when the initial load
ran**, so every historical date is a function of when someone last reseeded — not a stable fact about an order.
Reseed on a different day and a year of history silently slides.

The incremental path does not have this defect. `_build_incremental_orders` derives its day offset from a fixed
constant, `_EPOCH = date(2024, 1, 1)` (line 11, used at line 196), so reruns of the same date are idempotent.
The constant already exists; `_seed_initial` simply does not use it. That asymmetry is the whole bug.

### Why only one side could recover

- **Batch silver** rebuilds through the `first_run` branch (`generate_orders.py:49`) whenever the table is empty —
  a full `overwrite`. It re-derived everything from the post-reseed source and now matches.
- **SDP silver** is a `@dp.table` **streaming table**. It appends each row once and never revisits it. The freeze
  that exists deliberately to protect `product_name` from later renames also froze `order_date` and `country`.

**"One side fixed itself" is a clue, not a reassurance.** Batch is *newer*, not inherently *righter* — it agrees
with the current source only because it was rebuilt after the reseed.

### The unintended freeze (divergence 2)

An append-only table freezes **every column it appends**, not only the one the design was reasoning about. The
same commit froze `country`:

```
| customer_id | source_now | batch_frozen | sdp_frozen |    rows |
|-------------|------------|--------------|------------|---------|
|          51 | UK         | US           | UK         |  12,000 |
|          52 | UK         | US           | UK         |  12,000 |
|         ... |            |              |            |         |
```

`_seed_initial` resets `customer.country` to its banded default (ids 1–200 → US, 201–300 → UK, …), and each
incremental day MERGEs country changes onto a rotating window of customers. Batch silver, rebuilt at 12:12:00
right after the reset, holds the **banded default**. SDP silver holds the **accumulated mutations** of the prior
incarnation. Both froze; they froze different things.

447 of 500 customers are affected. Nobody designed `country` to be frozen — it came along for the ride.

---

## 6. Divergence 3 — the missing day (batch side)

Batch silver is missing **2026-07-24 entirely**:

```
| date       | curated.order_enriched | ..._sdp | raw.order |
|------------|------------------------|---------|-----------|
| 2026-07-22 |                  5,000 |   5,000 |     5,000 |
| 2026-07-23 |                  5,000 |   5,000 |     5,000 |
| 2026-07-24 |                      — |   5,000 |     5,000 |
| 2026-07-25 |                  5,000 |   5,000 |     5,000 |
```

`job1_prod` run **646670311225438** started 2026-07-24 09:01:50Z and **failed**:

> Task `extract_source2` failed with message: Workload failed, see run output for details. This caused all
> downstream tasks to get skipped.

That is the DQX quarantine schema-drift outage fixed by PR #52. The next run (1003939029330611, 2026-07-25
09:01:38Z) succeeded, and everything looked healthy again — **but the hole was never filled**:

- `raw.order` is a full `CREATE OR REPLACE` (v32 on 07-25 jumped 2,145,000 → 2,155,000, catching up *both* days),
  so bronze self-healed.
- `curated.order_enriched`'s incremental branch filters `raw.order` to **`date == seed_date`**
  (`generate_orders.py:62`) and MERGEs only that. The 07-25 run looked at 07-25 only. No later run ever looks
  back. Silver history jumps straight from v67 (07-23) to v70 (07-25).

The SDP path has no such hole: a streaming read consumes whatever is new, regardless of what date it carries.

**This is a live gap in a production gold table** — `report.order_agg` under-reports 2026-07-24 — and it is
independent of divergences 1 and 2. It is also the more likely one to recur: any failed daily run leaves a
permanent hole that no subsequent run repairs.

---

## 7. Divergence 4 — the tripled day (SDP side)

`external_source.order` received the 2026-06-24 incremental batch **twice** (v5 at 12:19:39, v7 at 15:55:05),
leaving 5,000 order ids with two copies each. Downstream:

- **Batch:** DQX's uniqueness check quarantined *both* copies — `raw.order_quarantine` holds exactly 10,000 rows
  over 5,000 ids, all dated 2026-06-24. Those orders had already been merged in at 12:21:45, so batch silver
  holds them once; the 15:57 MERGE inserted 0.
- **SDP:** no DQX stage. The streaming table appended 5,000 (commit 5) then 10,000 (commit 6), leaving each
  `(order_id, item_seq)` present **three** times — 5,000 keys × 3 = 15,000 rows.

This accounts for the entire 15,000-row count gap between the two silvers. It is an asymmetry by design (only
the batch path runs DQX), but it means the SDP gold table over-counts 2026-06-24.

---

## 8. Impact

| Table | Effect |
|---|---|
| `prod.curated.order_enriched_sdp` | 6M rows with `order_date` 13 days early and `country` from a dead incarnation; 2026-06-24 tripled |
| `prod.report.order_agg_sdp` | Inherits all of the above — the earliest 13 days of the window are fabricated |
| `prod.curated.order_enriched` | Missing 15,000 rows for 2026-07-24 |
| `prod.report.order_agg` | Under-reports 2026-07-24 |
| The AI/BI dashboard | Binds to the batch gold table, so it shows the 07-24 hole, not the SDP shift |

Any side-by-side batch/SDP comparison is currently misleading in **both** directions.

---

## 9. Fix options, and what each one costs

Divergences rarely have a free repair. State what moves, in both directions, before recommending anything.

**A. SDP full refresh — do not do this alone.** It re-derives `order_date` and `country` correctly, but a full
refresh of a streaming table discards **every frozen value it was carrying**, including `product_name`. Since
2026-06-24 the incremental seed has renamed products daily; a refresh relabels historically booked orders with
current names. That trades a divergence in two columns for a divergence in the one column the design exists to
protect — and the product-name freeze is the template's headline invariant.

**B. `make drop env=prod` + full rebuild.** Converges both paths and clears divergences 1–4 at once. But run
against today's code it **re-triggers the root cause**: the backfill re-anchors to the new run date, and prod's
entire history moves again. Only safe after C.

**C. Anchor `_seed_initial` on `_EPOCH` (recommended first step).** A one-line change at `seed_sources.py:135`
to derive the initial window from the same fixed constant the incremental path already uses. Makes the backfill
idempotent so a reseed reproduces identical dates. Fixes nothing already in prod on its own — it makes B safe.

**D. Backfill 2026-07-24 into batch silver.** Independent of A–C and much cheaper. Either run `job1` with
`seed_date=2026-07-24` (the incremental MERGE is insert-only and keyed on `(order_id, item_seq)`, so it is
idempotent and will not disturb other days), or widen the incremental filter from `date == seed_date` to a
lookback window so a failed day self-heals on the next run. The second is the real fix: today, **any** failed
daily run leaves a permanent hole.

**Recommended order: C → D → deploy → B.** C makes the rebuild safe, D closes the live gold-table gap without
waiting for a rebuild, and B is what actually converges the two paths.

Before executing B, confirm both writers are healthy — resetting a table whose writer is failing leaves it empty.

---

## 10. Standing lessons

- **A date derived from "now" at load time is not a fact.** Anything anchored on `current_date()`, a run date or
  a job parameter re-derives itself on every reload. No downstream reset fixes that permanently.
- **An append-only table freezes every column it appends.** The `country` divergence was collateral damage from
  a freeze designed for `product_name`. When a pipeline deliberately freezes one attribute, enumerate what else
  it is reading from the same static side.
- **A date-scoped incremental turns any failed run into a permanent hole.** The layer above self-healed because
  it was a full overwrite; the layer below did not because it only ever looks at one day.
- **`DESCRIBE HISTORY` refusing an object is information.** It told us `raw.order_sdp` is a materialized view,
  which is exactly why its intermediate state is unrecoverable.
- **Verify the premise, then keep looking.** The reported complaint was one shifted date column. Three more
  divergences turned up, one of them a live gap in a production gold table that nobody had reported.

---

## Appendix — queries

```sql
-- 1. Count down both paths (§1)
SELECT 'external_source.order' AS layer, COUNT(*) n, COUNT(DISTINCT id) k,
       MIN(date) min_d, MAX(date) max_d FROM prod.external_source.order
UNION ALL SELECT 'raw.order',     COUNT(*), COUNT(DISTINCT id), MIN(date), MAX(date) FROM prod.raw.order
UNION ALL SELECT 'raw.order_sdp', COUNT(*), COUNT(DISTINCT id), MIN(date), MAX(date) FROM prod.raw.order_sdp;

-- 2. Diff by key, with every other column checked in the same pass (§2)
SELECT datediff(b.order_date, s.order_date) AS delta_days, COUNT(*) n,
       COUNT(*) FILTER (WHERE b.product_name  <> s.product_name)  AS name_diff,
       COUNT(*) FILTER (WHERE b.item_total    <> s.item_total)    AS total_diff,
       COUNT(*) FILTER (WHERE b.order_total   <> s.order_total)   AS otot_diff,
       COUNT(*) FILTER (WHERE b.country       <> s.country)       AS country_diff,
       COUNT(*) FILTER (WHERE b.item_quantity <> s.item_quantity) AS qty_diff
FROM prod.curated.order_enriched b
JOIN prod.curated.order_enriched_sdp s USING (order_id, item_seq)
GROUP BY 1 ORDER BY n DESC;

-- 3. Pin the blast radius — not bounded by time-travel retention (§3)
SELECT _metadata.row_commit_version AS v, COUNT(*) n, MIN(order_date), MAX(order_date)
FROM prod.curated.order_enriched_sdp GROUP BY 1 ORDER BY 1;

-- 4. Timeline. Select operationParameters.mode explicitly: an overwrite is logged as
--    WRITE with mode='Overwrite', so filtering on `operation` alone hides it. (§4)
SELECT version, timestamp, operation, operationParameters.mode AS mode,
       operationMetrics.numOutputRows AS out_rows
FROM (DESCRIBE HISTORY prod.external_source.order) ORDER BY version;

-- 5. The country freeze: source now vs what each side froze (§5)
WITH d AS (
  SELECT b.customer_id, b.country AS batch_frozen, s.country AS sdp_frozen, COUNT(*) n
  FROM prod.curated.order_enriched b
  JOIN prod.curated.order_enriched_sdp s USING (order_id, item_seq)
  WHERE b.country <> s.country GROUP BY 1,2,3)
SELECT d.customer_id, c.country AS source_now, d.batch_frozen, d.sdp_frozen, d.n
FROM d JOIN prod.external_source.customer c ON c.id = d.customer_id ORDER BY d.customer_id;

-- 6. Rows on one side only — locates the missing day (§6)
SELECT order_id, item_seq FROM prod.curated.order_enriched_sdp
EXCEPT SELECT order_id, item_seq FROM prod.curated.order_enriched;
```
