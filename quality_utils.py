from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, lit, isnan, isnull, avg, stddev


# -------------------------------------------------
# 1. NULL CHECK (Column-level validation)
# -------------------------------------------------
def check_nulls(df: DataFrame, columns: list, threshold: float = 0.0):
    """
    Checks null percentage in columns.
    Fails if null ratio exceeds threshold.
    """

    total_count = df.count()

    for c in columns:

        null_count = df.filter(col(c).isNull() | isnan(col(c))).count()
        null_ratio = null_count / total_count

        if null_ratio > threshold:
            raise Exception(f"[NULL CHECK FAILED] {c} null ratio = {null_ratio}")


# -------------------------------------------------
# 2. DUPLICATE CHECK (Key integrity)
# -------------------------------------------------
def check_duplicates(df: DataFrame, keys: list):
    """
    Ensures uniqueness of business keys
    """

    dup_count = (
        df.groupBy(*keys)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if dup_count > 0:
        raise Exception(f"[DUPLICATE CHECK FAILED] Found {dup_count} duplicate groups")


# -------------------------------------------------
# 3. ROW COUNT CHECK (Before vs After ETL)
# -------------------------------------------------
def check_row_count(source_df: DataFrame, target_df: DataFrame, tolerance: float = 0.1):
    """
    Compares row counts between source and target
    """

    src_count = source_df.count()
    tgt_count = target_df.count()

    if src_count == 0:
        raise Exception("[ROW CHECK FAILED] Source is empty")

    diff_ratio = abs(src_count - tgt_count) / src_count

    if diff_ratio > tolerance:
        raise Exception(f"[ROW CHECK FAILED] Difference ratio = {diff_ratio}")


# -------------------------------------------------
# 4. SCHEMA DRIFT CHECK
# -------------------------------------------------
def check_schema(source_df: DataFrame, target_df: DataFrame):
    """
    Ensures schema consistency between datasets
    """

    src_cols = set(source_df.columns)
    tgt_cols = set(target_df.columns)

    missing_in_target = src_cols - tgt_cols
    extra_in_target = tgt_cols - src_cols

    if missing_in_target or extra_in_target:
        raise Exception(
            f"[SCHEMA DRIFT] Missing: {missing_in_target}, Extra: {extra_in_target}"
        )


# -------------------------------------------------
# 5. VALUE RANGE CHECK (Business rules)
# -------------------------------------------------
def check_value_range(df: DataFrame, column: str, min_value=None, max_value=None):
    """
    Validates numeric range constraints
    """

    if min_value is not None:
        invalid_count = df.filter(col(column) < min_value).count()
        if invalid_count > 0:
            raise Exception(f"[RANGE CHECK FAILED] {column} below min_value")

    if max_value is not None:
        invalid_count = df.filter(col(column) > max_value).count()
        if invalid_count > 0:
            raise Exception(f"[RANGE CHECK FAILED] {column} above max_value")


# -------------------------------------------------
# 6. BUSINESS RULE VALIDATION (Custom condition)
# -------------------------------------------------
def check_business_rule(df: DataFrame, condition, error_msg: str):
    """
    Generic business rule validator
    Example: balance > 0, age > 18
    """

    invalid_count = df.filter(~condition).count()

    if invalid_count > 0:
        raise Exception(f"[BUSINESS RULE FAILED] {error_msg}")


# -------------------------------------------------
# 7. DISTINCT VALUE CHECK (Data sanity)
# -------------------------------------------------
def check_distinct_count(df: DataFrame, column: str, expected_min: int = None):
    """
    Ensures column has expected diversity
    """

    distinct_count = df.select(column).distinct().count()

    if expected_min and distinct_count < expected_min:
        raise Exception(
            f"[DISTINCT CHECK FAILED] {column} has low diversity: {distinct_count}"
        )


# -------------------------------------------------
# 8. ZERO VALUE CHECK
# -------------------------------------------------
def check_zero_values(df: DataFrame, columns: list):
    """
    Checks unexpected zero values in numeric columns
    """

    for c in columns:
        zero_count = df.filter(col(c) == 0).count()

        if zero_count > 0:
            raise Exception(f"[ZERO VALUE WARNING] {c} contains {zero_count} zeros")


# -------------------------------------------------
# 9. OUTLIER DETECTION (Statistical check)
# -------------------------------------------------
def check_outliers(df: DataFrame, column: str, threshold: float = 3.0):
    """
    Detects outliers using Z-score method
    """

    stats = df.select(
        avg(col(column)).alias("mean"),
        stddev(col(column)).alias("stddev")
    ).collect()[0]

    mean_val = stats["mean"]
    stddev_val = stats["stddev"]

    if stddev_val == 0 or stddev_val is None:
        return

    outliers = df.filter(
        (col(column) > mean_val + threshold * stddev_val) |
        (col(column) < mean_val - threshold * stddev_val)
    ).count()

    if outliers > 0:
        raise Exception(f"[OUTLIER CHECK FAILED] {column} has {outliers} outliers")


# -------------------------------------------------
# 10. NULL COVERAGE REPORT (No failure, just metrics)
# -------------------------------------------------
def null_report(df: DataFrame) -> DataFrame:
    """
    Returns null percentage per column
    """

    total = df.count()

    result = []

    for c in df.columns:

        null_count = df.filter(col(c).isNull() | isnan(col(c))).count()
        null_pct = (null_count / total) * 100

        result.append((c, null_count, null_pct))

    return df.sparkSession.createDataFrame(
        result,
        ["column", "null_count", "null_percentage"]
    )


# -------------------------------------------------
# 11. DATA PROFILING (Quick dataset summary)
# -------------------------------------------------
def profile_data(df: DataFrame):
    """
    Returns basic dataset profiling info
    """

    return {
        "row_count": df.count(),
        "column_count": len(df.columns),
        "columns": df.columns
    }


# -------------------------------------------------
# 12. COMPOSITE DQ CHECK RUNNER (IMPORTANT)
# -------------------------------------------------
def run_dq_checks(df: DataFrame, rules: dict):
    """
    Executes multiple checks from config

    Example rules:
    {
        "not_null": ["id"],
        "duplicates": ["id"],
        "range": {"balance": (0, 100000)}
    }
    """

    if "not_null" in rules:
        check_nulls(df, rules["not_null"])

    if "duplicates" in rules:
        check_duplicates(df, rules["duplicates"])

    if "range" in rules:
        for col_name, (min_v, max_v) in rules["range"].items():
            check_value_range(df, col_name, min_v, max_v)
