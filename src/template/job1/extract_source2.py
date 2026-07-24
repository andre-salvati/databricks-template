from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import (
    Criticality,
    DQDatasetRule,
    DQForEachColRule,
    DQRowRule,
)

from ..baseTask import BaseTask


class ExtractSource2(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def validate_order(self, df_order):
        checks = [
            # Warning if total > 1000
            DQRowRule(
                column="total",
                check_func=check_funcs.is_not_greater_than,
                check_func_kwargs={"limit": 1000},
                criticality=Criticality.WARN.value,
            ),
            # Error if ids are not null or empty
            *DQForEachColRule(
                columns=["id", "id_customer"],
                check_func=check_funcs.is_not_null_and_not_empty,
                criticality=Criticality.ERROR.value,
                user_metadata={"check_type": "completeness", "responsible_data_steward": "someone@email.com"},
            ).get_rules(),
            # Error if id is not unique
            DQDatasetRule(
                columns=["id"],
                check_func=check_funcs.is_unique,
                criticality=Criticality.ERROR.value,
            ),
        ]

        df_valid, df_invalid = self.config.dq_engine.apply_checks_and_split(df_order, checks)

        return (df_valid, df_invalid)

    def run(self):
        self.logger.info("extracting data from Source2")

        quarantine_fail_ratio = self.config.get_value("quarantine_fail_ratio")

        df_order = self.spark.read.table("external_source.order")
        df_order, df_order_invalid = self.validate_order(df_order)

        # Persist quarantine first so it survives a downstream failure.
        #
        # overwriteSchema=true here, unlike every other medallion write. The quarantine table
        # carries DQX's `_errors`/`_warnings` structs, whose shape is owned by the DQX library
        # version, not by the source data contract — a DQX upgrade adds fields to them and would
        # otherwise hard-fail prod (it did: 0.15.0 added rule_fingerprint / rule_set_fingerprint /
        # skipped). That is a false positive, not schema drift. Real drift is still caught: the
        # business columns here come from the same DataFrame written to raw.order below, which
        # keeps overwriteSchema=false.
        (df_order_invalid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("raw.order_quarantine"))

        valid_count = df_order.count()
        invalid_count = df_order_invalid.count()
        total = valid_count + invalid_count
        ratio = (invalid_count / total) if total else 0.0
        self.logger.info("dq summary valid=%d invalid=%d ratio=%.3f", valid_count, invalid_count, ratio)

        # Hard-fail if too many rows are quarantined — silent quarantine bloat is the
        # main failure mode of DQX in production. Disabled by default (ratio=1.0).
        if ratio > quarantine_fail_ratio:
            raise RuntimeError(f"DQX quarantine ratio {ratio:.3f} exceeded threshold {quarantine_fail_ratio}")

        df_order_item = self.spark.read.table("external_source.order_item")

        (df_order.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("raw.order"))
        (df_order_item.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("raw.order_item"))
