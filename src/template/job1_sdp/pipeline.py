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
forces a full recompute.  Data quality validation for order data is handled by
job1's ExtractSource2 task (batch wheel) which runs before this pipeline.

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
- No DQX / quarantine split: order data flows unfiltered through all layers.
  Operators should rely on job1's quarantine tables for data quality monitoring.
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
    comment="Bronze: full copy of external_source.order.",
)
def raw_order_sdp():
    return spark.read.table(f"{_catalog}.external_source.order")


# ── Bronze: raw.order_item_sdp ────────────────────────────────────────────────


@dp.materialized_view(
    name=f"{_catalog}.raw.order_item_sdp",
    comment="Bronze: full copy of external_source.order_item (mirrors ExtractSource1).",
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
