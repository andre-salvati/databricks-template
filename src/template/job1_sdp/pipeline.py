"""
job1_sdp — Spark Declarative Pipeline equivalent of the job1 batch ETL.

All output tables carry the ``_sdp`` suffix so they coexist with the batch
job1 tables in the same catalog without overwriting each other.

Data flow (all batch tables / materialized views — full reprocess per run):

  external_source.customer   ──►  raw.customer_sdp               (@dp.table)
  external_source.order_item ──►  raw.order_item_sdp             (@dp.table)

  external_source.order ──► [_order_dq temp view]
                                  ├──► raw.order_sdp             (@dp.table, valid rows)
                                  └──► raw.order_quarantine_sdp   (@dp.table, invalid rows)

  raw.{customer,order,order_item}_sdp ──► curated.order_enriched_sdp (@dp.materialized_view)

  curated.order_enriched_sdp ──► report.order_agg_sdp            (@dp.materialized_view)

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
- Quarantine fail-ratio hard-fail: omitted — count() is prohibited inside SDP
  pipeline functions.  Operators should monitor raw.order_quarantine_sdp row
  counts via a Lakeflow quality metric or an external alert instead.
- health_check / seed_sources: out of scope — these are orchestration/ops tasks,
  not part of the ETL data-flow.
"""

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark import pipelines as dp

from template.job1_sdp.transforms import aggregate_orders, enrich_order, validate_order

# ── Catalog resolution ────────────────────────────────────────────────────────
# target_catalog is stamped into the pipeline configuration by the generator
# (sdk_generate_sdp_pipeline.py) at deploy time, so no runtime user-lookup is
# needed here. spark is injected by the SDP runtime.
_catalog = spark.conf.get("target_catalog")
_ws = WorkspaceClient()
_dq = DQEngine(_ws)

# ── Bronze: raw.customer_sdp ──────────────────────────────────────────────────


@dp.table(
    name=f"{_catalog}.raw.customer_sdp",
    comment="Bronze: full copy of external_source.customer (mirrors ExtractSource1).",
)
def raw_customer_sdp():
    return spark.read.table(f"{_catalog}.external_source.customer")


# ── Bronze: raw.order_sdp + raw.order_quarantine_sdp (DQX split) ─────────────


@dp.temporary_view()
def _order_dq():
    """
    Annotate external_source.order with DQX _errors / _warnings columns.

    Pipeline-scoped temp view; not persisted to the catalog.  Downstream
    raw.order_sdp and raw.order_quarantine_sdp both read from this view so
    DQX checks run only once per pipeline update.
    """
    df = spark.read.table(f"{_catalog}.external_source.order")
    return validate_order(_dq, df)


@dp.table(
    name=f"{_catalog}.raw.order_sdp",
    comment="Bronze: valid orders from external_source.order (DQX-filtered, no errors).",
)
def raw_order_sdp():
    # get_valid() removes the _errors/_warnings columns and returns only clean rows.
    return _dq.get_valid(spark.read.table("_order_dq"))


@dp.table(
    name=f"{_catalog}.raw.order_quarantine_sdp",
    comment="Bronze: quarantined orders rejected by DQX checks (rows with _errors).",
)
def raw_order_quarantine_sdp():
    # get_invalid() returns rows that have errors or warnings, including the
    # _errors/_warnings columns so operators can inspect failure reasons.
    return _dq.get_invalid(spark.read.table("_order_dq"))


# ── Bronze: raw.order_item_sdp ────────────────────────────────────────────────


@dp.table(
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
