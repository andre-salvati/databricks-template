"""
job1_sdp — Spark Declarative Pipeline equivalent of the job1 batch ETL.

All output tables carry the ``_sdp`` suffix so they coexist with the batch
job1 tables in the same catalog without overwriting each other.

Data flow:

  external_source.customer   ──►  raw.customer_sdp    (@dp.materialized_view, simple copy)
  external_source.order      ──►  raw.order_sdp       (@dp.materialized_view, simple copy)
  external_source.order_item ──►  raw.order_item_sdp  (@dp.materialized_view, simple copy)

  raw.customer_sdp ─┐
  raw.order_sdp     ├──► curated.order_enriched_sdp   (@dp.materialized_view)
  raw.order_item_sdp┘

  curated.order_enriched_sdp ──► report.order_agg_sdp  (@dp.materialized_view)

All tables are @dp.materialized_view so Enzyme can track Delta version watermarks
end-to-end and perform incremental refresh on every layer.

DQX is intentionally absent from this pipeline — apply_checks() stamps run_time
timestamps on violation structs, which Enzyme treats as non-deterministic and
forces a full recompute.

Data quality notes
------------------
- raw.order_sdp filters out rows with null id or id_customer (Enzyme-safe row
  filter).  These rows would silently drop in the silver inner join anyway, but
  filtering at bronze makes the exclusion explicit and avoids misleading nulls
  in the bronze layer.
- Duplicate order ids are NOT filtered here: detecting duplicates requires a
  full-dataset scan (COUNT OVER PARTITION BY), which Enzyme treats as
  non-incrementalizable and forces a full refresh — the opposite of the goal.
  Operators should monitor job1's raw.order_quarantine table for duplicate-id
  alerts; that quarantine is produced by ExtractSource2's DQX is_unique check.
- This pipeline reads external_source.order directly, not raw.order (the
  DQX-filtered batch output).  It runs independently of job1 with no ordering
  guarantee, so the above filter is the only structural data-quality guard on
  the SDP path.

Catalog resolution
------------------
``target_catalog`` is resolved at **generate time** by sdk_generate_template_job.py
(using WorkspaceClient, mirroring Config.__init__) and stamped into the pipeline's
configuration block.  pipeline.py reads it via ``spark.conf.get("target_catalog")``
— no runtime WorkspaceClient call is needed for catalog lookup.

  - dev      → dev_<sanitized_user>  (per-developer sandbox)
  - staging  → staging
  - prod     → prod

Known differences from job1
---------------------------
- Table names: all SDP output tables have a ``_sdp`` suffix so they coexist
  with the batch job1 tables in the same catalog.
- No quarantine split: null id/id_customer rows are filtered at bronze (see
  above); duplicate ids are not filtered (Enzyme constraint) — monitor
  job1's raw.order_quarantine for those.
- health_check / seed_sources: out of scope — these are orchestration/ops tasks,
  not part of the ETL data-flow.
"""

from pyspark import pipelines as dp

from template.job1_sdp.transforms import aggregate_orders, enrich_order

# ── Catalog resolution ────────────────────────────────────────────────────────
# target_catalog is stamped into the pipeline configuration by the generator
# (sdk_generate_template_job.py) at deploy time, so no runtime user-lookup is
# needed here. spark is injected by the SDP runtime.
_catalog = spark.conf.get("target_catalog")

# ── Bronze: raw.customer_sdp ──────────────────────────────────────────────────


@dp.materialized_view(
    name=f"{_catalog}.raw.customer_sdp",
    comment="Bronze: full copy of external_source.customer (mirrors ExtractSource1).",
)
def raw_customer_sdp():
    return spark.read.table(f"{_catalog}.external_source.customer")


# ── Bronze: raw.order_sdp ─────────────────────────────────────────────────────


@dp.materialized_view(
    name=f"{_catalog}.raw.order_sdp",
    comment="Bronze: external_source.order with null id/id_customer rows excluded. "
    "Duplicate ids are not filtered (requires full-dataset scan, blocks Enzyme incremental refresh).",
)
def raw_order_sdp():
    return spark.read.table(f"{_catalog}.external_source.order").filter("id IS NOT NULL AND id_customer IS NOT NULL")


# ── Bronze: raw.order_item_sdp ────────────────────────────────────────────────


@dp.materialized_view(
    name=f"{_catalog}.raw.order_item_sdp",
    comment="Bronze: full copy of external_source.order_item (mirrors ExtractSource2).",
)
def raw_order_item_sdp():
    return spark.read.table(f"{_catalog}.external_source.order_item")


# ── Silver: curated.order_enriched_sdp ───────────────────────────────────────


@dp.materialized_view(
    name=f"{_catalog}.curated.order_enriched_sdp",
    comment="Silver: order_item ⨝ order ⨝ customer join (mirrors GenerateOrders).",
)
def curated_order_enriched_sdp():
    return enrich_order(
        spark.read.table(f"{_catalog}.raw.customer_sdp"),
        spark.read.table(f"{_catalog}.raw.order_sdp"),
        spark.read.table(f"{_catalog}.raw.order_item_sdp"),
    )


# ── Gold: report.order_agg_sdp ───────────────────────────────────────────────


@dp.materialized_view(
    name=f"{_catalog}.report.order_agg_sdp",
    comment="Gold: total qty and value per customer name (mirrors GenerateOrdersAgg).",
)
def report_order_agg_sdp():
    return aggregate_orders(spark.read.table(f"{_catalog}.curated.order_enriched_sdp"))
