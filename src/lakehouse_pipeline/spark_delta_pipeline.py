"""Optional PySpark + Delta Lake implementation of the same merge design.

This module is intentionally separate from the lightweight unit-test path. Run it in
Databricks or a local Spark environment installed with the project's `spark` extra.
"""

from __future__ import annotations

import argparse

KEY_COLUMNS = ["source_system", "patient_id", "referral_type", "referral_date"]
CONTENT_COLUMNS = [
    "source_system",
    "patient_id",
    "referral_type",
    "referral_date",
    "priority",
    "status",
    "facility_region",
]


def build_source_dataframe(spark, input_path: str):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    source = spark.read.json(input_path)

    source = (
        source.withColumn(
            "record_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[F.lower(F.trim(F.col(column).cast("string"))) for column in KEY_COLUMNS],
                ),
                256,
            ),
        )
        .withColumn(
            "row_hash",
            F.sha2(F.to_json(F.struct(*[F.col(column) for column in CONTENT_COLUMNS])), 256),
        )
    )

    latest = Window.partitionBy("record_id").orderBy(F.col("source_updated_at").desc())
    return source.withColumn("_row_number", F.row_number().over(latest)).filter("_row_number = 1").drop("_row_number")


def run_pipeline(input_path: str, target_path: str, audit_path: str, run_id: str) -> None:
    from delta import configure_spark_with_delta_pip
    from delta.tables import DeltaTable
    from pyspark.sql import SparkSession, functions as F

    builder = (
        SparkSession.builder.appName("reliable-lakehouse-pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    source = build_source_dataframe(spark, input_path).cache()
    source_count = source.count()

    if not DeltaTable.isDeltaTable(spark, target_path):
        initial = (
            source.withColumn("pipeline_run_id", F.lit(run_id))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("last_modified_at", F.current_timestamp())
        )
        initial.write.format("delta").mode("overwrite").save(target_path)
        inserted, updated, unchanged = source_count, 0, 0
    else:
        target_df = spark.read.format("delta").load(target_path).select("record_id", "row_hash")
        comparison = source.alias("s").join(target_df.alias("t"), "record_id", "left")
        inserted = comparison.filter(F.col("t.row_hash").isNull()).count()
        updated = comparison.filter(F.col("t.row_hash").isNotNull() & (F.col("s.row_hash") != F.col("t.row_hash"))).count()
        unchanged = comparison.filter(F.col("t.row_hash").isNotNull() & (F.col("s.row_hash") == F.col("t.row_hash"))).count()

        delta_target = DeltaTable.forPath(spark, target_path)
        updates = (
            source.withColumn("pipeline_run_id", F.lit(run_id))
            .withColumn("last_modified_at", F.current_timestamp())
            .withColumn("ingested_at", F.current_timestamp())
        )

        update_set = {column: f"source.{column}" for column in source.columns}
        update_set.update(
            {
                "pipeline_run_id": "source.pipeline_run_id",
                "last_modified_at": "source.last_modified_at",
            }
        )

        (
            delta_target.alias("target")
            .merge(updates.alias("source"), "target.record_id = source.record_id")
            .whenMatchedUpdate(condition="target.row_hash <> source.row_hash", set=update_set)
            .whenNotMatchedInsertAll()
            .execute()
        )

    audit = spark.createDataFrame(
        [(run_id, source_count, inserted, updated, unchanged)],
        "pipeline_run_id string, source_rows long, inserted long, updated long, unchanged long",
    ).withColumn("processed_at", F.current_timestamp())
    audit.write.format("delta").mode("append").save(audit_path)
    source.unpersist()
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--audit", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    run_pipeline(args.input, args.target, args.audit, args.run_id)


if __name__ == "__main__":
    main()
