from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, sha2, concat_ws


# -------------------------------------------------
# 1. Simple full row deduplication
# -------------------------------------------------
def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """
    Removes fully identical rows
    """
    return df.dropDuplicates()


# -------------------------------------------------
# 2. Key-based deduplication (keep first row)
# -------------------------------------------------
def dedup_by_keys(df: DataFrame, keys: list) -> DataFrame:
    """
    Removes duplicates based on keys, keeps arbitrary first record
    """
    return df.dropDuplicates(keys)


# -------------------------------------------------
# 3. Keep latest record based on timestamp
# -------------------------------------------------
def dedup_keep_latest(
    df: DataFrame,
    keys: list,
    order_by_col: str
) -> DataFrame:
    """
    Keeps latest record per key using a timestamp column
    """

    window_spec = Window.partitionBy(*keys).orderBy(col(order_by_col).desc())

    return (
        df.withColumn("rn", row_number().over(window_spec))
          .filter(col("rn") == 1)
          .drop("rn")
    )


# -------------------------------------------------
# 4. Keep highest priority record
# -------------------------------------------------
def dedup_by_priority(
    df: DataFrame,
    keys: list,
    priority_col: str
) -> DataFrame:
    """
    Keeps record with highest priority value (e.g. 1 = highest)
    """

    window_spec = Window.partitionBy(*keys).orderBy(col(priority_col).asc())

    return (
        df.withColumn("rn", row_number().over(window_spec))
          .filter(col("rn") == 1)
          .drop("rn")
    )


# -------------------------------------------------
# 5. Hash-based dedup (for large datasets)
# -------------------------------------------------
def dedup_by_hash(df: DataFrame, subset_cols: list = None) -> DataFrame:
    """
    Creates hash of selected columns and removes duplicates
    """

    if subset_cols is None:
        subset_cols = df.columns

    df_hashed = df.withColumn(
        "row_hash",
        sha2(concat_ws("||", *[col(c).cast("string") for c in subset_cols]), 256)
    )

    window_spec = Window.partitionBy("row_hash").orderBy(col("row_hash"))

    return (
        df_hashed.withColumn("rn", row_number().over(window_spec))
                 .filter(col("rn") == 1)
                 .drop("rn", "row_hash")
    )


# -------------------------------------------------
# 6. Conditional dedup (business rule based)
# -------------------------------------------------
def dedup_by_condition(df: DataFrame, condition_col: str, keys: list) -> DataFrame:
    """
    Keeps rows where condition_col = TRUE first, otherwise fallback
    Example: is_active = 1 preferred
    """

    window_spec = Window.partitionBy(*keys).orderBy(col(condition_col).desc())

    return (
        df.withColumn("rn", row_number().over(window_spec))
          .filter(col("rn") == 1)
          .drop("rn")
    )
