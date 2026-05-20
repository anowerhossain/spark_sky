from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, rand, concat
import math


# -------------------------------------------------
# 1. Fixed Repartition (Standard ETL)
# -------------------------------------------------
def repartition_fixed(df: DataFrame, num_partitions: int = 200) -> DataFrame:
    return df.repartition(num_partitions)


# -------------------------------------------------
# 2. Repartition by Key (JOIN / AGG optimization)
# -------------------------------------------------
def repartition_by_key(df: DataFrame, keys: list) -> DataFrame:
    return df.repartition(*[col(k) for k in keys])


# -------------------------------------------------
# 3. Range Partitioning (Time-series / DWH)
# -------------------------------------------------
def repartition_by_range(df: DataFrame, column: str, num_partitions: int = 200) -> DataFrame:
    return df.repartition(num_partitions, col(column))


# -------------------------------------------------
# 4. Coalesce Before Write (avoid small files)
# -------------------------------------------------
def coalesce_before_write(df: DataFrame, num_partitions: int = 50) -> DataFrame:
    return df.coalesce(num_partitions)


# -------------------------------------------------
# 5. Smart Partitioning (Auto sizing based on data)
# -------------------------------------------------
def smart_partition(df: DataFrame, target_size_mb: int = 128) -> DataFrame:
    """
    Approximate auto partitioning based on row count.
    NOTE: In production, replace estimation with actual size metrics.
    """

    row_count = df.count()

    estimated_size_mb = row_count / 100000  # heuristic

    num_partitions = max(1, math.ceil(estimated_size_mb / target_size_mb))

    return df.repartition(num_partitions)


# -------------------------------------------------
# 6. Skew Handling (Salting technique)
# -------------------------------------------------
def repartition_with_skew_handling(df: DataFrame, key: str, salt_range: int = 10) -> DataFrame:
    """
    Handles data skew by adding salt to key
    """

    return df.withColumn(
        "salted_key",
        concat(col(key).cast("string"), lit("_"), (rand() * salt_range).cast("int"))
    ).repartition("salted_key")


# -------------------------------------------------
# 7. Bucket-like Partitioning (Hive / Iceberg optimization)
# -------------------------------------------------
def bucket_partition(df: DataFrame, key: str, num_buckets: int = 16) -> DataFrame:
    return df.repartition(num_buckets, col(key))


# -------------------------------------------------
# 8. Partition Alignment (Hive / Iceberg tables)
# -------------------------------------------------
def align_partition(df: DataFrame, partition_col: str) -> DataFrame:
    return df.repartition(col(partition_col))


# -------------------------------------------------
# 9. Write Optimization Partitioning Strategy
# -------------------------------------------------
def optimize_for_write(df: DataFrame, mode: str = "medium") -> DataFrame:
    """
    Controls partitioning before write operations
    """

    if mode == "small":
        return df.coalesce(1)

    elif mode == "medium":
        return df.coalesce(20)

    elif mode == "large":
        return df.repartition(200)

    return df


# -------------------------------------------------
# 10. Partition Debug Utility
# -------------------------------------------------
def show_partition_distribution(df: DataFrame, key: str = None):
    """
    Debug partition count and distribution
    """

    print("Total Partitions:", df.rdd.getNumPartitions())

    if key:
        df.groupBy(key).count().show(50, False)


# -------------------------------------------------
# 11. Dynamic Partition by Column List
# -------------------------------------------------
def dynamic_repartition(df: DataFrame, columns: list, num_partitions: int = 200) -> DataFrame:
    """
    Repartition based on multiple columns dynamically
    """

    return df.repartition(num_partitions, *[col(c) for c in columns])


# -------------------------------------------------
# 12. High-Performance Join Partitioning Helper
# -------------------------------------------------
def prep_for_join(df: DataFrame, join_keys: list, broadcast_hint: bool = False) -> DataFrame:
    """
    Prepares dataframe for join performance optimization
    """

    df = df.repartition(*[col(k) for k in join_keys])

    return df


# -------------------------------------------------
# 13. Heavy Aggregation Optimization Partitioning
# -------------------------------------------------
def prep_for_aggregation(df: DataFrame, group_cols: list) -> DataFrame:
    """
    Optimizes dataframe before groupBy / aggregation
    """

    return df.repartition(*[col(c) for c in group_cols])


# -------------------------------------------------
# 14. Partition Count Auto Suggestion (Diagnostics)
# -------------------------------------------------
def suggest_partitions(df: DataFrame, rows_per_partition: int = 200000) -> int:
    """
    Suggests ideal partition count based on dataset size
    """

    total_rows = df.count()

    return max(1, total_rows // rows_per_partition)
