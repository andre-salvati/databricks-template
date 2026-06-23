# Data model

The medallion layout, the catalog/schema isolation model, table schemas, the attribute-freeze
semantics, liquid clustering, and data quality. For how tasks/jobs are wired see
[architecture.md](architecture.md).

## Catalog / schema model (load-bearing)

**Environment isolation is at the *catalog* level, not the schema level.** The same medallion
schemas exist in every catalog.

- `dev_{sanitized_user}` — per-developer sandbox, created lazily by `Config.__init__`. Username is
  `current_user.me().user_name.split("@")[0]` with non-alphanumerics → `_` (e.g.
  `andre.f.salvati` → `andre_f_salvati`).
- `staging`, `prod` — shared; provisioned upfront by `make init` (`scripts/sdk_init_workspace.py`),
  which creates the catalogs, all `MEDALLION_SCHEMAS`, and the grants. Runtime jobs in these envs
  must **not** have `CREATE CATALOG` / `CREATE SCHEMA` privilege — that belongs to bootstrap, not
  the runtime wheel. (In `config.py`, `CREATE` calls live only under the `args.env == "dev"` branch.)

Medallion schemas (`MEDALLION_SCHEMAS` in `config.py`):

| Schema | Layer | Content |
|---|---|---|
| `external_source` | source | Raw input — `seed_sources` (prod, daily) or the integration `setup` task (dev/staging). |
| `raw` | bronze | Direct copies from sources. |
| `curated` | silver | Joined/enriched tables. |
| `report` | gold | Aggregated tables. |
| `ops` | internal | Health-check table. Named `ops` because UC reserves `system`. |

Each task's input/output tables are **hardcoded** in the task module (e.g. `raw.customer` →
`curated.order_enriched`) — the dbt `ref()` pattern. The medallion layer is a semantic contract,
not a runtime parameter; don't parameterize it.

## Medallion data flow

Two parallel paths run from the same source tables. Edge labels are write modes; dotted edges are
static-dimension reads or the DQX quarantine branch; per-table labels show cluster keys and
MV-vs-streaming-table.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': {'primaryColor': '#e0e0e0', 'primaryBorderColor': '#999999', 'primaryTextColor': '#000000', 'clusterBkg': '#d0d0d0', 'clusterBorder': '#999999'}}}%%
flowchart LR

subgraph EXT["external_source"]
    direction TB
    ec["customer · no cluster"]
    ep["product · cluster: product_id"]
    eo["order · cluster: date"]
    ei["order_item · cluster: id_order"]
end

subgraph BATCH["job1 · batch"]
    direction LR

    subgraph RAW_B["raw · no cluster"]
        direction TB
        rc["customer"]
        rp["product"]
        ro["order"]
        rq["order_quarantine"]
        ri["order_item"]
    end

    subgraph CUR_B["curated"]
        cb["order_enriched · cluster: order_date"]
    end

    subgraph REP_B["report"]
        rb["order_agg · cluster: order_date, product_id"]
    end

    rc --> cb
    rp --> cb
    ro -->|"1st: overwrite / incr: MERGE insert-only (order_id, item_seq)"| cb
    ri --> cb
    cb -->|"1st: overwrite / incr: replaceWhere order_date"| rb
end

subgraph SDP_G["job1_sdp · declarative"]
    direction LR

    subgraph RAW_S["raw"]
        direction TB
        sc["customer_sdp · no cluster · MV"]
        sp["product_sdp · cluster: product_id · MV"]
        so["order_sdp · cluster: date · MV"]
        si["order_item_sdp · cluster: id_order · streaming table"]
    end

    subgraph CUR_S["curated"]
        cs["order_enriched_sdp · cluster: order_date · streaming table"]
    end

    subgraph REP_S["report"]
        rs["order_agg_sdp · cluster: order_date, product_id · MV"]
    end

    sc -. static dim .-> cs
    sp -. static dim .-> cs
    so -. static dim .-> cs
    si -->|"streaming append (stream-static join)"| cs
    cs -->|MV refresh| rs
end

ec -->|overwrite| rc
ep -->|overwrite| rp
eo -->|"overwrite + DQX"| ro
eo -. DQX quarantine .-> rq
ei -->|"overwrite + DQX"| ri

ec -->|MV refresh| sc
ep -->|MV refresh| sp
eo -->|MV refresh| so
ei -->|streaming append| si
```

- **`raw.*` batch is intentionally unclustered** — full daily overwrite means clustering can't amortize.
- **`raw.*_sdp` picks up cluster keys from the decorator** — set at table creation; no `ALTER TABLE`.
- **Dashed arrows into `order_enriched_sdp`** are static dimension reads; only `order_item_sdp` drives the stream.
- **`external_source` clustering** is set by `seed_sources._ensure_tables()` in prod and by
  `integration_setup.cluster_by()` calls in dev/staging.

## Table schemas

Canonical schemas live in `commonSchemas.py` (`order_enriched_schema`, `order_agg_schema`,
`product_schema`).

- **`curated.order_enriched`**: `customer_name, country, customer_id, order_id, order_total,
  order_date (DateType), product_id, product_name, product_category_id, category_name, item_seq,
  item_description, item_quantity, item_total`.
- **`report.order_agg`**: `customer_name, country, order_date (DateType), product_id, product_name,
  product_category_id, category_name, total_quantity, total_value, total_orders`.

`total_value` in gold is `SUM(item_total)` — the line value the source froze on the order at sale
time, so a later price change never restates historical revenue. `product_name` (`"Product 1"`) and
`category_name` (`"Category 2"`, derived as `concat('Category ', product_category_id)`) are
human-readable labels carried alongside the numeric ids; the dashboard displays the labels.
`product_name` is **frozen** per row at first processing (see below) — it is the mutable attribute the
freeze pattern is demonstrated against.

## Incremental silver: product-name freeze (load-bearing)

`external_source.product` is a mutable dimension — the daily seed renames a couple of products each
run (`"Product 1"` → `"Product 1.1"` → `"Product 1.2"`, suffix = cumulative rename count). The
pipeline freezes `product_name` onto each order line at sale time, so a later rename never relabels
already-booked orders (`unit_price` is a static attribute and `total_value = SUM(item_total)`, so
revenue is never restated either). **Both** pipelines freeze, by different mechanisms:

- **`job1` (batch)** — `generate_orders` does first-run-full / incremental-after: first run (silver
  empty) overwrites all backfilled orders; every subsequent run enriches only `date = seed_date`
  orders and **`MERGE … WHEN NOT MATCHED THEN INSERT`** (insert-only, never update) keyed on
  `(order_id, item_seq)`. Gold mirrors this: first-run full overwrite, then `replaceWhere
  order_date = DATE'<seed_date>'`.
- **`job1_sdp` (declarative)** — silver (`curated.order_enriched_sdp`) and bronze `raw.order_item_sdp`
  are **streaming tables** (`@dp.table` + `spark.readStream`). A stream–static join (streaming
  `order_item` fact ⨝ static dims) appends each row once and never reprocesses it, so `product_name`
  is frozen on append. **A materialized view would restate** the name on every refresh — that is
  why silver had to become a streaming table. Gold stays an MV because it re-sums already-frozen silver.

Why an MV restates but a streaming table freezes: an MV is *defined as a query over current inputs*
and recomputes from scratch (latest-wins); a streaming table consumes new input rows once and
appends. "Incremental" (Enzyme re-reading only changed files) is efficiency, not semantics. The
dashboard consolidates the "by product" chart by `product_id` (labeled with each product's latest
name), so a renamed product stays one line; the frozen historical names remain in `report.order_agg`
and in the Product filter. Known limitations (acceptable for a template): the first-run backfill
freezes the *current* name; freeze is at *processing* time, not strictly *order date*; country is
frozen at append time in SDP.

## Liquid clustering

Clustering keys are set on the **accumulating** tables only (`raw.*` batch are full-overwritten
daily, so clustering can't amortize — left unclustered): `external_source.order` (`date`),
`external_source.order_item` (`id_order`), `external_source.product` (`product_id`),
`curated.order_enriched` (`order_date`), `report.order_agg` (`order_date, product_id`). Batch tasks
call `BaseTask.cluster_by(table, *cols)` (idempotent `ALTER TABLE … CLUSTER BY`) after writing; SDP
tables pass `cluster_by=[...]` to the `@dp.table` / `@dp.materialized_view` decorator. Clustering is
metadata-only — it does not rewrite existing data.

## Data quality (DQX)

`ExtractSource2` demonstrates the DQX pattern: define rules as `DQRowRule` / `DQForEachColRule` /
`DQDatasetRule`, call `dq_engine.apply_checks_and_split()`, write invalid rows to a `_quarantine`
table. The `--quarantine-fail-ratio` job parameter hard-fails the task when too many rows are
quarantined (silent quarantine bloat is the main DQX failure mode in prod).

<img src="../assets/data_quality.png">

## Data lineage (Unity Catalog)

Unity Catalog captures table-to-table lineage across the medallion layers automatically.

<img src="../assets/data_lineage.png">
