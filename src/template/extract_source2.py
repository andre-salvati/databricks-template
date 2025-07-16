from databricks.labs.dqx import check_funcs
from databricks.labs.dqx.rule import (
    Criticality,
    DQDatasetRule,
    DQForEachColRule,
    DQRowRule,
)

from .baseTask import BaseTask


class ExtractSource2(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def validate_order(self, df_order):
        checks = [
            # Warning if total > 150
            DQRowRule(
                column="total",
                check_func=check_funcs.is_not_greater_than,
                check_func_kwargs={"limit": 150},
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
        print("Extracting data from Source2 ...")

        df_order = self.spark.read.table("external_source.order")
        df_order, df_order_invalid = self.validate_order(df_order)
        df_order_invalid.write.mode("overwrite").saveAsTable(f"{self.config.get_value('schema')}.order_quarantine")

        df_order_item = self.spark.read.table("external_source.order_item")

        if self.config.get_value("debug"):
            df_order.show()
            df_order_invalid.show()
            df_order_item.show()

        df_order.write.mode("overwrite").saveAsTable(f"{self.config.get_value('schema')}.order")
        df_order_item.write.mode("overwrite").saveAsTable(f"{self.config.get_value('schema')}.order_item")
