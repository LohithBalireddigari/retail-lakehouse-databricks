from pyspark.sql import functions as F

from retail_lakehouse import quality


def test_split_by_rules_separates_rows(spark):
    df = spark.createDataFrame([(1, 10), (2, -5), (None, 3)], "id INT, qty INT")
    rules = {"id_not_null": F.col("id").isNotNull(), "qty_positive": F.col("qty") > 0}

    valid, quarantine = quality.split_by_rules(df, rules)

    assert [r.id for r in valid.collect()] == [1]
    failed = {r.qty: r._failed_rules for r in quarantine.collect()}
    assert failed[-5] == ["qty_positive"]
    assert failed[3] == ["id_not_null"]


def test_null_rule_result_counts_as_failure(spark):
    df = spark.createDataFrame([(None,)], "qty INT")
    valid, quarantine = quality.split_by_rules(df, {"qty_positive": F.col("qty") > 0})
    assert valid.count() == 0
    assert quarantine.count() == 1


def test_report_pass_rate(spark):
    df = spark.createDataFrame([(1,), (2,), (3,), (-1,)], "qty INT")
    valid, quarantine = quality.split_by_rules(df, {"pos": F.col("qty") > 0})
    report = quality.build_report("x", valid, quarantine)
    assert (report.total, report.valid, report.quarantined, report.pass_rate) == (4, 3, 1, 0.75)


def test_rule_failure_counts(spark):
    df = spark.createDataFrame([(None, -1), (None, 1)], "id INT, qty INT")
    _, quarantine = quality.split_by_rules(
        df, {"id_not_null": F.col("id").isNotNull(), "qty_positive": F.col("qty") > 0}
    )
    counts = {r.rule: r["count"] for r in quality.rule_failure_counts(quarantine).collect()}
    assert counts == {"id_not_null": 2, "qty_positive": 1}
