"""Data-quality rules: split rows into valid and quarantined sets.

Each rule is a boolean Column that is True when the row passes. A null result
counts as a failure, so missing values never slip through silently.
"""

from dataclasses import dataclass
from typing import Dict, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

Rules = Dict[str, Column]

EMAIL_REGEX = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"


def split_by_rules(df: DataFrame, rules: Rules) -> Tuple[DataFrame, DataFrame]:
    """Return ``(valid, quarantine)``.

    ``quarantine`` keeps every original column plus ``_failed_rules`` (array of
    rule names) and ``_quarantined_at``.
    """
    if not rules:
        return df, df.limit(0).withColumn("_failed_rules", F.array().cast("array<string>"))

    checks = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    tagged = df.withColumn("_failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))

    valid = tagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    quarantine = tagged.filter(F.size("_failed_rules") > 0).withColumn(
        "_quarantined_at", F.current_timestamp()
    )
    return valid, quarantine


@dataclass
class QualityReport:
    entity: str
    total: int
    valid: int
    quarantined: int

    @property
    def pass_rate(self) -> float:
        return 1.0 if self.total == 0 else round(self.valid / self.total, 4)

    def as_dict(self) -> dict:
        return {
            "entity": self.entity,
            "total": self.total,
            "valid": self.valid,
            "quarantined": self.quarantined,
            "pass_rate": self.pass_rate,
        }


def build_report(entity: str, valid: DataFrame, quarantine: DataFrame) -> QualityReport:
    v, q = valid.count(), quarantine.count()
    return QualityReport(entity=entity, total=v + q, valid=v, quarantined=q)


def rule_failure_counts(quarantine: DataFrame) -> DataFrame:
    """How many rows failed each rule (one row may fail several)."""
    return (
        quarantine.select(F.explode("_failed_rules").alias("rule"))
        .groupBy("rule")
        .count()
        .orderBy(F.desc("count"))
    )


# ---------------------------------------------------------------------------
# Rule sets per entity (applied to already-typed Silver candidates)
# ---------------------------------------------------------------------------

def customer_rules() -> Rules:
    return {
        "customer_id_not_null": F.col("customer_id").isNotNull(),
        "email_valid": F.col("email").rlike(EMAIL_REGEX),
        "signup_date_parsed": F.col("signup_date").isNotNull(),
    }


def product_rules() -> Rules:
    return {
        "product_id_not_null": F.col("product_id").isNotNull(),
        "unit_price_positive": F.col("unit_price") > 0,
        "category_present": F.col("category").isNotNull() & (F.length("category") > 0),
    }


def order_rules() -> Rules:
    return {
        "order_id_not_null": F.col("order_id").isNotNull(),
        "customer_id_not_null": F.col("customer_id").isNotNull(),
        "order_ts_parsed": F.col("order_ts").isNotNull(),
        "status_known": F.col("status").isin("COMPLETED", "SHIPPED", "PENDING", "CANCELLED", "RETURNED"),
    }


def order_item_rules() -> Rules:
    return {
        "order_item_id_not_null": F.col("order_item_id").isNotNull(),
        "quantity_positive": F.col("quantity") > 0,
        "unit_price_non_negative": F.col("unit_price") >= 0,
        "discount_in_range": F.col("discount_pct").between(0, 1),
    }
